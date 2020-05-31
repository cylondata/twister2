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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.gson.reflect.TypeToken;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.config.SchedulerContext;
import edu.iu.dsc.tws.api.scheduler.IUploader;
import edu.iu.dsc.tws.api.scheduler.UploaderException;
import edu.iu.dsc.tws.master.JobMasterContext;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesConstants;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesContext;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesController;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesUtils;
import edu.iu.dsc.tws.rsched.schedulers.k8s.RequestObjectBuilder;
import edu.iu.dsc.tws.rsched.utils.JobUtils;

import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.Configuration;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.util.Watch;
import okhttp3.OkHttpClient;

/**
 * Upload job package to either:
 *   uploader web server pods
 *   all job pods
 *
 * If there are uploader web server pods in the cluster, job package is uploaded to all those pods.
 * After uploading is finished, uploader completes.
 *
 * If there are no uploader web servers,
 * the job package is uploaded to each pod in the job separately.
 *   we watch the job pods for both workers and the job master
 *   when a pod becomes Running, we transfer the job package to that pod.
 *   This class runs in the submitting client.
 *   It needs to run continually in the client to upload the job package in case of scaling up
 *   or pod failures. If the job is neither scalable nor fault tolerant,
 *   then it can exit after uploading the package to all pods.
 *
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
public class K8sUploader extends Thread implements IUploader {
  private static final Logger LOG = Logger.getLogger(K8sUploader.class.getName());

  public static final long MAX_WAIT_TIME_FOR_POD_START = 300 * 1000L; // in seconds

  private CoreV1Api coreApi;
  private ApiClient apiClient;

  private Config config;
  private String namespace;
  private JobAPI.Job job;
  private String jobID;
  private String localJobPackageFile;
  private List<String> webServerPodNames;

  private static boolean uploadToWebServers;

  private ArrayList<String> podNames;
  private HashMap<String, UploaderToPod> initialPodUploaders = new HashMap<>();
  private ArrayList<UploaderToPod> uploaders = new ArrayList<>();

  private ArrayList<UploaderToPod> uploadersToWebServers = new ArrayList<>();

  private Watch<V1Pod> watcher;
  private boolean stopUploader = false;
  private long watcherStartTime = System.currentTimeMillis();

  public K8sUploader() {
  }

  @Override
  public void initialize(Config cnfg, JobAPI.Job jb) {

    this.config = cnfg;
    this.namespace = KubernetesContext.namespace(config);
    this.job = jb;
    this.jobID = job.getJobId();
    webServerPodNames = KubernetesController.getUploaderWebServerPods(
        namespace, KubernetesContext.uploaderWebServerLabel(config));

    if (webServerPodNames.size() == 0) {
      uploadToWebServers = false;
      // set upload method in RequestObjectBuilder
      RequestObjectBuilder.setUploadMethod("client-to-pods");
    } else {
      uploadToWebServers = true;
    }
  }

  @Override
  public URI uploadPackage(String sourceLocation) throws UploaderException {

    localJobPackageFile = sourceLocation + "/" + SchedulerContext.jobPackageFileName(config);

    // start uploader thread
    start();

    if (uploadToWebServers) {
      String uri = KubernetesContext.uploaderWebServer(config) + "/"
          + JobUtils.createJobPackageFileName(jobID);
      try {
        return new URI(uri);
      } catch (URISyntaxException e) {
        LOG.log(Level.SEVERE, "Can not generate URI for uploader web server: " + uri, e);
        throw new UploaderException("Can not generate URI for download link: " + uri, e);
      }
    }
    return null;
  }

  @Override
  public void run() {
    createApiInstances();

    if (uploadToWebServers) {
      startUploadersToWebServers();
    } else {
      watchPodsStartUploaders();
    }
  }

  private void createApiInstances() {

    try {
      apiClient = io.kubernetes.client.util.Config.defaultClient();
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Exception when creating ApiClient: ", e);
      throw new RuntimeException(e);
    }

    OkHttpClient httpClient =
        apiClient.getHttpClient().newBuilder().readTimeout(0, TimeUnit.SECONDS).build();
    apiClient.setHttpClient(httpClient);
    Configuration.setDefaultApiClient(apiClient);
    coreApi = new CoreV1Api(apiClient);
  }

  private void startUploadersToWebServers() {

    String targetFile = KubernetesUtils.jobPackageFullPath(config, jobID);

    for (String webServerPodName: webServerPodNames) {
      UploaderToPod uploader =
          new UploaderToPod(namespace, webServerPodName, localJobPackageFile, targetFile);
      uploader.start();
      uploadersToWebServers.add(uploader);
    }
  }

  private boolean completeFileTransfersToWebServers() {
    // wait all transfer threads to finish up
    boolean allTransferred = true;
    for (UploaderToPod uploader: uploadersToWebServers) {

      try {
        uploader.join();
        if (!uploader.packageTransferred()) {
          LOG.log(Level.SEVERE, "Job Package is not transferred to the web server pod: "
              + uploader.getPodName());
          allTransferred = false;
          break;
        }
      } catch (InterruptedException e) {
        LOG.log(Level.WARNING, "Thread sleep interrupted.", e);
      }
    }

    // if one transfer fails, tell all transfer threads to stop and return false
    if (!allTransferred) {
      for (UploaderToPod uploader: uploadersToWebServers) {
        uploader.cancelTransfer();
      }
    }

    return allTransferred;
  }

  /**
   * watch job pods until they become Running and start an uploader for each pod afterward
   */
  private void watchPodsStartUploaders() {

    /** Pod Phases: Pending, Running, Succeeded, Failed, Unknown
     * ref: https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-phase */

    podNames = KubernetesUtils.generatePodNames(job);
    // add job master pod name
    if (!JobMasterContext.jobMasterRunsInClient(config)) {
      podNames.add(KubernetesUtils.createJobMasterPodName(job.getJobId()));
    }

    String jobPodsLabel = KubernetesUtils.jobLabelSelector(jobID);
    String targetFile = KubernetesConstants.POD_MEMORY_VOLUME
        + "/" + JobUtils.createJobPackageFileName(jobID);

    Integer timeoutSeconds = Integer.MAX_VALUE;
    String podPhase = "Running";

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
            && podPhase.equals(item.object.getStatus().getPhase())
        ) {
          String podName = item.object.getMetadata().getName();

//        LOG.info(runningCounter++ + " -------------- Pod event: " + podName + ", " + podPhase);

          // if DeletionTimestamp is not null,
          // it means that the pod is in the process of being deleted
          if (item.object.getMetadata().getDeletionTimestamp() == null) {
            UploaderToPod uploader =
                new UploaderToPod(namespace, podName, localJobPackageFile, targetFile);
            uploader.start();

            if (podNames.contains(podName)) {
              podNames.remove(podName);
              initialPodUploaders.put(podName, uploader);
            } else {
              uploaders.add(uploader);
            }
          }
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
   * wait for all transfer threads to finish
   * if any one of them fails, stop all active ones
   *
   * When upload method is "client-to-pods" and
   * dynamic IDriver is used, this thread never finishes
   * It waits in case a new worker is added by IDriver
   * User should finish this process with control-C
   // TODO: may be we can watch the job somehow and exit it when the job completes
   * @return
   */
  @Override
  public boolean complete() {

    if (uploadToWebServers) {
      return completeFileTransfersToWebServers();
    }

    // wait until all transfer threads to be started
    while (!podNames.isEmpty()) {

      long duration = System.currentTimeMillis() - watcherStartTime;
      if (duration > MAX_WAIT_TIME_FOR_POD_START) {
        LOG.log(Level.SEVERE, "Max wait time limit has been reached and not all pods started.");
        return false;
      }

      try {
        Thread.sleep(300);
      } catch (InterruptedException e) {
        LOG.log(Level.WARNING, "Thread sleep interrupted.", e);
      }
    }

    // wait all transfer threads to finish up
    boolean allTransferred = true;
    for (Map.Entry<String, UploaderToPod> entry: initialPodUploaders.entrySet()) {

      try {
        entry.getValue().join();
        if (!entry.getValue().packageTransferred()) {
          LOG.log(Level.SEVERE, "Job Package is not transferred to the pod: " + entry.getKey());
          allTransferred = false;
          break;
        }
      } catch (InterruptedException e) {
        LOG.log(Level.WARNING, "Thread sleep interrupted.", e);
      }
    }

    // if one transfer fails, tell all transfer threads to stop and return false
    if (!allTransferred) {
      for (Map.Entry<String, UploaderToPod> entry: initialPodUploaders.entrySet()) {
        entry.getValue().cancelTransfer();
      }
    }

    if (!JobUtils.isJobScalable(config, job) || !allTransferred) {
      stopUploader();
    }

    return allTransferred;
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

    for (Map.Entry<String, UploaderToPod> entry: initialPodUploaders.entrySet()) {
      entry.getValue().cancelTransfer();
    }

    for (UploaderToPod uploader: uploaders) {
      uploader.cancelTransfer();
    }
  }

  @Override
  public boolean undo(Config cnfg, String jbID) {
    stopUploader();
    return false;
  }

  @Override
  public void close() {

  }
}
