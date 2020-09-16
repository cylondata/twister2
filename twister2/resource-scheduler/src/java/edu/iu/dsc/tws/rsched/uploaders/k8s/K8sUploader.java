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

import java.net.URI;
import java.util.List;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.scheduler.IUploader;
import edu.iu.dsc.tws.api.scheduler.UploaderException;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesContext;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesController;
import edu.iu.dsc.tws.rsched.schedulers.k8s.RequestObjectBuilder;

/**
 * Upload the job package to either:
 *    uploader web server pods with UploaderToWebServers
 *    all job pods with DirectUploader
 * <p>
 * If there are uploader web server pods in the cluster,
 * upload the job package to all those pods.
 * Uploader completes after the uploading is finished.
 * Uploader web server pods must have the label "app=twister2-uploader"
 * This label can be set from the config with the parameter:
 *    "twister2.kubernetes.uploader.web.server.label"
 * <p>
 * If there are no uploader web servers,
 * The job package is uploaded to each pod in the job directly.
 * DirectUploader watches the job pods for both workers and the job master
 * when a pod becomes Running, a thread transfers the job package to that pod.
 *
 * This class runs in the submitting client.
 * It needs to run continually in the client until the job ends,
 * to upload the job package in case of scaling up or pod failures.
 */
public class K8sUploader implements IUploader {
  private static final Logger LOG = Logger.getLogger(K8sUploader.class.getName());

  private IUploader uploader;

  public K8sUploader() {
  }

  @Override
  public void initialize(Config cnfg, String jbID) {

    KubernetesController controller = KubernetesController.init(KubernetesContext.namespace(cnfg));
    List<String> webServerPodNames = controller.getUploaderWebServerPods(
        KubernetesContext.uploaderWebServerLabel(cnfg));

    if (webServerPodNames.size() == 0) {
      // set upload method in RequestObjectBuilder
      RequestObjectBuilder.setUploadMethod("client-to-pods");
      uploader = new DirectUploader();
    } else {
      uploader = new UploaderToWebServers(webServerPodNames);
    }
    uploader.initialize(cnfg, jbID);
  }

  @Override
  public URI uploadPackage(String sourceLocation) throws UploaderException {
    return uploader.uploadPackage(sourceLocation);
  }

  @Override
  public boolean complete() {
    return uploader.complete();
  }

  @Override
  public boolean undo() {
    return uploader.undo();
  }

  @Override
  public void close() {
    uploader.close();
  }
}
