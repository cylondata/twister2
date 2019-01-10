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
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.gson.reflect.TypeToken;

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
public class UploaderForScaler extends Thread {
  private static final Logger LOG = Logger.getLogger(UploaderForScaler.class.getName());

  private CoreV1Api coreApi;
  private ApiClient apiClient;

  private String namespace;
  private String jobName;
  private String ssName;
  private String jobPackageFile;

  // this shows the initial number of pods in statefulset
  // ignore this many Running messages initially
  private int initialPods;

  public UploaderForScaler(String namespace,
                           String jobName,
                           String ssName,
                           String jobPackageFile,
                           int initialPods) {
    this.namespace = namespace;
    this.jobName = jobName;
    this.ssName = ssName;
    this.jobPackageFile = jobPackageFile;
    this.initialPods = initialPods;
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

    String workerRoleLabel = KubernetesUtils.createWorkerRoleLabelWithKey(jobName);

    Integer timeoutSeconds = Integer.MAX_VALUE;
    Watch<V1Pod> watch = null;
    String podPhase = "Running";

    try {
      watch = Watch.createWatch(
          apiClient,
          coreApi.listNamespacedPodCall(namespace, null, null, null, null, workerRoleLabel,
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

    int runningCounter = 0;
    for (Watch.Response<V1Pod> item : watch) {

      if (item.object != null
          && item.object.getMetadata().getName().startsWith(ssName)
          && podPhase.equals(item.object.getStatus().getPhase())
      ) {
        String podName = item.object.getMetadata().getName();

//        LOG.info(runningCounter++ + " -------------- Pod event: " + podName + ", " + podPhase);

        // if DeletionTimestamp is not null,
        // it means that the pod is in the process of being deleted
        if (runningCounter >= initialPods
            && item.object.getMetadata().getDeletionTimestamp() == null) {
          UploaderToWorker uploader = new UploaderToWorker(namespace, podName, jobPackageFile);
          uploader.start();
        }
      }
    }

    try {
      watch.close();
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Exception closing watcher.", e);
    }

  }

}
