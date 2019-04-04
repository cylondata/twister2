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
package edu.iu.dsc.tws.rsched.schedulers.k8s.uploader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.gson.reflect.TypeToken;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.master.JobMasterContext;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesContext;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesUtils;

import io.kubernetes.client.ApiClient;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.Configuration;
import io.kubernetes.client.apis.CoreV1Api;
import io.kubernetes.client.models.V1Pod;
import io.kubernetes.client.util.Watch;

/**
 * watch scalable StatefulSet in a job
 * when a pod is added by scaling up,
 * transfer the job package to that worker
 * This class runs in the submitting client
 * It needs to run continually in the client to upload the job package in case of scaling up
 *
 * Note:
 * There is a problem with pod Running state
 * When a pod deleted by scaling down, two Running state messages are generated
 * This is unfortunate. It may be a bug.
 * Currently I try to upload the job package with each Running message
 * If the pod is being deleted, it does not succeed.
 * So in failure case, I do not print log message.
 * I only print log message in success case.
 * Not accurate but a temporary solution.
 */
public class UploaderForJob extends Thread {
  private static final Logger LOG = Logger.getLogger(UploaderForJob.class.getName());

  public static final long MAX_WAIT_TIME_FOR_POD_START = 300 * 1000L; // in seconds

  private CoreV1Api coreApi;
  private ApiClient apiClient;

  private Config config;
  private String namespace;
  private JobAPI.Job job;
  private String jobName;
  private String jobPackageFile;

  private ArrayList<String> podNames;
  private HashMap<String, UploaderToPod> initialPodUploaders = new HashMap<>();
  private ArrayList<UploaderToPod> uploaders = new ArrayList<>();

  private Watch<V1Pod> watcher;
  private boolean stopUploader = false;
  private long watcherStartTime = System.currentTimeMillis();

  public UploaderForJob(Config config,
                        JobAPI.Job job,
                        String jobPackageFile) {

    this.config = config;
    this.namespace = KubernetesContext.namespace(config);
    this.job = job;
    this.jobName = job.getJobName();
    this.jobPackageFile = jobPackageFile;

    podNames = KubernetesUtils.generatePodNames(job);
    // add job master pod name
    if (!JobMasterContext.jobMasterRunsInClient(config)) {
      podNames.add(KubernetesUtils.createJobMasterPodName(job.getJobName()));
    }
  }

  @Override
  public void run() {
    createApiInstances();
    watchScaledUpPods();
  }

  private void createApiInstances() {

    try {
      apiClient = io.kubernetes.client.util.Config.defaultClient();
      apiClient.getHttpClient().setReadTimeout(0, TimeUnit.MILLISECONDS);
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Exception when creating ApiClient: ", e);
      throw new RuntimeException(e);
    }
    Configuration.setDefaultApiClient(apiClient);

    coreApi = new CoreV1Api(apiClient);
  }


  private void watchScaledUpPods() {

    /** Pod Phases: Pending, Running, Succeeded, Failed, Unknown
     * ref: https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-phase */

    String jobPodsLabel = KubernetesUtils.createJobPodsLabelWithKey(jobName);

    Integer timeoutSeconds = Integer.MAX_VALUE;
    String podPhase = "Running";

    try {
      watcher = Watch.createWatch(
          apiClient,
          coreApi.listNamespacedPodCall(namespace, null, null, null, null, jobPodsLabel,
              null, null, timeoutSeconds, Boolean.TRUE, null, null),
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
            && item.object.getMetadata().getName().startsWith(jobName)
            && podPhase.equals(item.object.getStatus().getPhase())
        ) {
          String podName = item.object.getMetadata().getName();

//        LOG.info(runningCounter++ + " -------------- Pod event: " + podName + ", " + podPhase);

          // if DeletionTimestamp is not null,
          // it means that the pod is in the process of being deleted
          if (item.object.getMetadata().getDeletionTimestamp() == null) {
            UploaderToPod uploader = new UploaderToPod(namespace, podName, jobPackageFile);
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
   * @return
   */
  public boolean completeFileTransfers() {

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

    if (!isJobScalable() || !allTransferred) {
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

  /**
   * For the job to be scalable:
   *   Driver class shall be specified
   *   a scalable compute resource shall be given
   *   itshould not be an openMPI job
   *
   * @return
   */
  public boolean isJobScalable() {

    // if Driver is not set, it means there is nothing to scale the job
    if (job.getDriverClassName().isEmpty()) {
      return false;
    }

    // if there is no scalable compute resource in the job, can not be scalable
    boolean computeResourceScalable =
        job.getComputeResource(job.getComputeResourceCount() - 1).getScalable();
    if (!computeResourceScalable) {
      return false;
    }

    // if it is an OpenMPI job, it is not scalable
    if (SchedulerContext.useOpenMPI(config)) {
      return false;
    }

    return true;
  }


}
