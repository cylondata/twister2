//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
package edu.iu.dsc.tws.rsched.uploaders.k8s;

import java.io.File;
import java.net.URI;
import java.util.List;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.config.SchedulerContext;
import edu.iu.dsc.tws.api.scheduler.IUploader;
import edu.iu.dsc.tws.api.scheduler.UploaderException;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesContext;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesController;
import edu.iu.dsc.tws.rsched.schedulers.k8s.RequestObjectBuilder;

/**
 * Upload job package to either:
 * uploader web server pods
 * all job pods
 * <p>
 * If there are uploader web server pods in the cluster, job package is uploaded to all those pods.
 * After uploading is finished, uploader completes.
 * <p>
 * If there are no uploader web servers,
 * the job package is uploaded to each pod in the job separately.
 * we watch the job pods for both workers and the job master
 * when a pod becomes Running, we transfer the job package to that pod.
 * This class runs in the submitting client.
 * It needs to run continually in the client to upload the job package in case of scaling up
 * or pod failures. If the job is neither scalable nor fault tolerant,
 * then it can exit after uploading the package to all pods.
 * <p>
 * Note:
 * There is a problem with pod Running state
 * When a pod is deleted by scaling down, two Running state messages are generated.
 * This is unfortunate. It may be a bug.
 * Currently I try to upload the job package with each Running message.
 * If the pod is being deleted, it does not succeed.
 * So in failure case, I do not print log message.
 * I only print log message in success case.
 * Not accurate but a temporary solution.
 */
public class K8sUploader implements IUploader {
  private static final Logger LOG = Logger.getLogger(K8sUploader.class.getName());

  private UploaderToWebServers wsUploader;
  private DirectUploader directUploader;

  private boolean uploadToWebServers;
  private List<String> webServerPodNames;
  private Config config;

  private boolean initialized = false;

  public K8sUploader() {
  }

  @Override
  public void initialize(Config cnfg, JobAPI.Job jb) {
    this.config = cnfg;
    initialized = true;

    webServerPodNames = KubernetesController.getUploaderWebServerPods(
        KubernetesContext.namespace(cnfg), KubernetesContext.uploaderWebServerLabel(cnfg));

    if (webServerPodNames.size() == 0) {
      uploadToWebServers = false;
      // set upload method in RequestObjectBuilder
      RequestObjectBuilder.setUploadMethod("client-to-pods");
      directUploader = new DirectUploader(cnfg, jb);
    } else {
      uploadToWebServers = true;
      wsUploader = new UploaderToWebServers(cnfg, jb.getJobId(), webServerPodNames);
    }
  }

  @Override
  public URI uploadPackage(String sourceLocation) throws UploaderException {

    String localJobPackageFile = sourceLocation + File.separator
        + SchedulerContext.jobPackageFileName(config);

    if (uploadToWebServers) {
      return wsUploader.uploadPackage(localJobPackageFile);
    } else {
      return directUploader.uploadPackage(localJobPackageFile);
    }
  }

  @Override
  public boolean complete() {

    if (uploadToWebServers) {
      return wsUploader.complete();
    } else {
      return directUploader.complete();
    }
  }

  @Override
  public boolean undo(Config cnfg, String jbID) {

    // if initialize method is not called
    if (!initialized) {
      webServerPodNames = KubernetesController.getUploaderWebServerPods(
          KubernetesContext.namespace(cnfg), KubernetesContext.uploaderWebServerLabel(cnfg));

      if (webServerPodNames.size() == 0) {
        // if it is DirectUploader,
        // nothing to undo
        return true;
      } else {
        uploadToWebServers = true;
        wsUploader = new UploaderToWebServers(cnfg, jbID, webServerPodNames);
      }
    }

    if (uploadToWebServers) {
      return wsUploader.undo(cnfg, jbID);
    } else {
      return directUploader.undo(cnfg, jbID);
    }
  }

  @Override
  public void close() {
    if (uploadToWebServers) {
      wsUploader.close();
    } else {
      directUploader.close();
    }
  }
}
