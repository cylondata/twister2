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

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.gson.reflect.TypeToken;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.scheduler.UploaderException;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesConstants;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesContext;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesController;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesUtils;
import edu.iu.dsc.tws.rsched.utils.JobUtils;

import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.util.Watch;

public class DirectUploader extends Thread {
  private static final Logger LOG = Logger.getLogger(DirectUploader.class.getName());

  private CoreV1Api coreApi;
  private ApiClient apiClient;

  private Config config;
  private String namespace;
  private JobAPI.Job job;
  private String jobID;
  private String localJobPackageFile;

  private ArrayList<UploaderToPod> uploaders = new ArrayList<>();

  private Watch<V1Pod> watcher;
  private boolean stopUploader = false;
  private JobEndWatcher jobEndWatcher;

  public DirectUploader(Config cnfg, JobAPI.Job jb) {

    this.config = cnfg;
    this.namespace = KubernetesContext.namespace(config);
    this.job = jb;
    this.jobID = job.getJobId();
  }

  public URI uploadPackage(String localJobPackage) throws UploaderException {

    localJobPackageFile = localJobPackage;
    KubernetesController controller = KubernetesController.init(namespace);
    apiClient = KubernetesController.getApiClient();
    coreApi = KubernetesController.createCoreV1Api();

    // start uploader thread
    start();

    // initialize job watcher
    jobEndWatcher = JobEndWatcher.init(config, jobID, controller, this);

    return null;
  }

  private void printLog() {
    String logMsg = System.lineSeparator() + System.lineSeparator();
    logMsg += "Twister2 Client will upload the job package directly to the job pods.\n";
    logMsg += "Twister2 Client needs to run until the job completes. \n";
    logMsg += "###########   Please do not kill the Twister2 Client   ###########\n";
    logMsg += System.lineSeparator();
    LOG.info(logMsg);
  }

  /**
   * watch job pods until they become Running and start an uploader for each pod afterward
   */
  @Override
  public void run() {

    printLog();

    String jobPodsLabel = KubernetesUtils.jobLabelSelector(jobID);
    String targetFile = KubernetesConstants.POD_MEMORY_VOLUME
        + "/" + JobUtils.createJobPackageFileName(jobID);

    Integer timeoutSeconds = Integer.MAX_VALUE;

    try {
      watcher = Watch.createWatch(
          apiClient,
          coreApi.listNamespacedPodCall(namespace, null, null, null, null, jobPodsLabel,
              null, null, timeoutSeconds, Boolean.TRUE, null),
          new TypeToken<Watch.Response<V1Pod>>() {
          }.getType());

    } catch (ApiException e) {
      String logMessage = "Exception when watching the pods to get the IPs: \n"
          + "exCode: " + e.getCode() + "\n"
          + "responseBody: " + e.getResponseBody();
      LOG.log(Level.SEVERE, logMessage, e);
      throw new RuntimeException(e);
    }

    // when we close the watcher to stop uploader,
    // it throws RuntimeException
    // we catch this exception and ignore it.
    try {

      for (Watch.Response<V1Pod> item : watcher) {

        if (stopUploader) {
          break;
        }

        if (item.object != null
            && item.object.getMetadata().getName().startsWith(jobID)
            && KubernetesUtils.isPodRunning(item.object)) {

          String podName = item.object.getMetadata().getName();
          UploaderToPod uploader =
              new UploaderToPod(namespace, podName, localJobPackageFile, targetFile);
          uploader.start();
          uploaders.add(uploader);
        }
      }

    } catch (RuntimeException e) {
      if (stopUploader) {
        LOG.fine("Uploader is stopped.");
        return;
      } else {
        throw e;
      }
    }

    closeWatcher();
  }

  /**
   * DirectUploader should run until the job completes
   * it should upload the job package in case of failures and job scaling up new workers
   */
  public boolean complete() {
    return true;
  }

  private void closeWatcher() {

    if (watcher == null) {
      return;
    }

    try {
      watcher.close();
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Exception closing watcher.", e);
    }

    watcher = null;
  }

  public void stopUploader() {
    stopUploader = true;
    closeWatcher();

    for (UploaderToPod uploader : uploaders) {
      uploader.cancelTransfer();
    }

    if (jobEndWatcher != null) {
      jobEndWatcher.stopWatcher();
    }
  }

  public boolean undo(Config cnfg, String jbID) {
    stopUploader();
    return true;
  }

  public void close() {
  }

}
