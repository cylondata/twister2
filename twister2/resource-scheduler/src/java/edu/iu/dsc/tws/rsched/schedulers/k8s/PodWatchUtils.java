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
package edu.iu.dsc.tws.rsched.schedulers.k8s;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.gson.reflect.TypeToken;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;

import io.kubernetes.client.ApiClient;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.Configuration;
import io.kubernetes.client.apis.CoreV1Api;
import io.kubernetes.client.models.V1Event;
import io.kubernetes.client.models.V1Pod;
import io.kubernetes.client.models.V1PodList;
import io.kubernetes.client.util.Watch;

/**
 * this class is used to provide methods related to watching pods in a job
 */
public final class PodWatchUtils {
  private static final Logger LOG = Logger.getLogger(PodWatchUtils.class.getName());

  private static CoreV1Api coreApi;
  private static ApiClient apiClient;

  private PodWatchUtils() {
  }

  public static void createApiInstances() {

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

  /**
   * watch pods until getting the Running event for Job Master pod
   * return its IP address
   */
  public static String getJobMasterIP(Config cnfg, int timeout) {

    String jobName = SchedulerContext.jobName(cnfg);
    String jobMasterPodName = KubernetesUtils.createJobMasterPodName(jobName);
    String namespace = KubernetesContext.namespace(cnfg);

    ArrayList<String> jobMasterNameAsList = new ArrayList<>();
    jobMasterNameAsList.add(jobMasterPodName);

    String serviceLabel = KubernetesUtils.createJobMasterServiceLabelWithKey(jobName);

    HashMap<String, String> nameAndIP =
        discoverRunningPodIPs(jobMasterNameAsList, namespace, serviceLabel, timeout);

    if (nameAndIP == null) {
      return null;
    }

    return nameAndIP.get(jobMasterPodName);
  }

  /**
   * watch pods until getting the Running event for all the pods in the given list
   * return pod names and IP addresses as a HashMap
   */
  public static HashMap<String, String> getRunningWorkerPodIPs(ArrayList<String> podNames,
                                                               String jobName,
                                                               String namespace,
                                                               int timeout) {

    String serviceLabel = KubernetesUtils.createServiceLabelWithKey(jobName);
    return discoverRunningPodIPs(podNames, namespace, serviceLabel, timeout);
  }

  /**
   * this is used to get IP addresses of both worker and job master pods
   * watch pods until getting the Running event for all the pods in the given list
   * return pod names and IP addresses as a HashMap
   */
  public static HashMap<String, String> getRunningJobPodIPs(ArrayList<String> podNames,
                                                            String jobName,
                                                            String namespace,
                                                            int timeout) {

    String jobPodsLabel = KubernetesUtils.createJobPodsLabelWithKey(jobName);
    return discoverRunningPodIPs(podNames, namespace, jobPodsLabel, timeout);
  }

  /**
   * watch pods until getting the Running event for all the pods in the given list
   * return pod names and IP addresses as a HashMap
   */
  public static HashMap<String, String> discoverRunningPodIPs(ArrayList<String> podNames,
                                                              String namespace,
                                                              String labelSelector,
                                                              int timeout) {

    /** Pod Phases: Pending, Running, Succeeded, Failed, Unknown
     * ref: https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-phase */

    if (apiClient == null || coreApi == null) {
      createApiInstances();
    }

    HashMap<String, String> podNamesIPs = new HashMap<>();

    String phase = "Running";
    Integer timeoutSeconds = timeout;
    Watch<V1Pod> watch = null;

    try {
      watch = Watch.createWatch(
          apiClient,
          coreApi.listNamespacedPodCall(namespace, null, null, null, null, labelSelector,
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

    boolean allPodsRunning = false;

    for (Watch.Response<V1Pod> item : watch) {

      if (item.object != null
          && podNames.contains(item.object.getMetadata().getName())
          && phase.equals(item.object.getStatus().getPhase())) {

        String podName = item.object.getMetadata().getName();

        // remove the pod from the list
        podNames.remove(podName);

        // add the pod to pod hashmap
        String podIP = item.object.getStatus().getPodIP();
        podNamesIPs.put(podName, podIP);

        LOG.info("Received pod Running event for the pod: " + podName + "[" + podIP + "]");

        if (podNames.size() == 0) {
          allPodsRunning = true;
          break;
        }
      }
    }

    try {
      watch.close();
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Exception closing watcher.", e);
    }

    return allPodsRunning ? podNamesIPs : null;
  }

  /**
   * watch pods until getting the Running event for all the pods in the given map
   * mark each pod that reached to Running state in the given map
   */
  public static boolean watchPodsToRunning(String namespace,
                                           String jobName,
                                           HashMap<String, Boolean> pods,
                                           int timeout) {

    /** Pod Phases: Pending, Running, Succeeded, Failed, Unknown
     * ref: https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-phase */

    if (apiClient == null || coreApi == null) {
      createApiInstances();
    }

    String phase = "Running";
    String serviceLabel = KubernetesUtils.createServiceLabelWithKey(jobName);
    Integer timeoutSeconds = timeout;
    Watch<V1Pod> watch = null;

    try {
      watch = Watch.createWatch(
          apiClient,
          coreApi.listNamespacedPodCall(namespace, null, null, null, null, serviceLabel,
              null, null, timeoutSeconds, Boolean.TRUE, null, null),
          new TypeToken<Watch.Response<V1Pod>>() {
          }.getType());

    } catch (ApiException e) {
      String logMessage = "Exception when watching the pods for the job: " + jobName + "\n"
          + "exCode: " + e.getCode() + "\n"
          + "responseBody: " + e.getResponseBody();
      LOG.log(Level.SEVERE, logMessage, e);
      throw new RuntimeException(e);
    }

    boolean allPodsRunning = false;

    for (Watch.Response<V1Pod> item : watch) {

      if (item.object != null
          && pods.containsKey(item.object.getMetadata().getName())
          && phase.equals(item.object.getStatus().getPhase())) {

        String podName = item.object.getMetadata().getName();
        pods.put(podName, true);

        LOG.log(Level.INFO, "Received pod Running event for the pod: " + podName);

        if (allTrue(pods.values())) {
          LOG.log(Level.INFO, "All pods reached Running state.");
          allPodsRunning = true;
          break;
        }
      }
    }

    try {
      watch.close();
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Exception closing watcher.", e);
    }

    return allPodsRunning;
  }

  private static boolean allTrue(Collection<Boolean> flags) {
    for (Boolean flag : flags) {
      if (!flag) {
        return false;
      }
    }

    return true;
  }

  /**
   * watch all pods in the given list until they become Starting
   * flag the pods with a true value in the given HashMap
   */
  public static boolean watchPodsToStarting(String namespace,
                                            HashMap<String, Boolean> pods,
                                            int timeout) {

    /** Event Reasons: SuccessfulMountVolume, Killing, Scheduled, Pulled, Created, Started
     * ref: https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-phase */

    if (apiClient == null || coreApi == null) {
      createApiInstances();
    }

    String reason = "Started";
    Integer timeoutSeconds = timeout;
    Watch<V1Event> watch = null;

    try {
      watch = Watch.createWatch(
          apiClient,
          coreApi.listNamespacedEventCall(namespace, null, null, null, null, null,
              null, null, timeoutSeconds, Boolean.TRUE, null, null),
          new TypeToken<Watch.Response<V1Event>>() {
          }.getType());

    } catch (ApiException e) {
      LOG.log(Level.SEVERE, "Can not start event watcher for the namespace: " + namespace, e);
      return false;
    }

    boolean allPodsStarted = false;

    for (Watch.Response<V1Event> item : watch) {
      if (item.object != null && reason.equals(item.object.getReason())) {

        String involvedPod = item.object.getInvolvedObject().getName();
        if (pods.containsKey(involvedPod) && !pods.get(involvedPod)) {
          pods.put(involvedPod, true);
          LOG.log(Level.INFO, "Container started event received for the pod: " + involvedPod);

          if (allTrue(pods.values())) {
            LOG.log(Level.INFO, "All pods reached Starting state.");
            allPodsStarted = true;
            break;
          }
        }
      }
    }

    try {
      watch.close();
    } catch (IOException e) {
      LOG.log(Level.WARNING, "Exception when clsoing the watcher.", e);
    }

    return allPodsStarted;
  }

  /**
   * get the IP of the node where the pod with that name is running
   * @param namespace
   * @return
   */
  public static String getNodeIP(String namespace, String jobName, String podIP) {

    if (apiClient == null || coreApi == null) {
      createApiInstances();
    }

    String jobPodsLabel = KubernetesUtils.createJobPodsLabelWithKey(jobName);

    V1PodList podList = null;
    try {
      podList = coreApi.listNamespacedPod(
          namespace, null, null, null, null, jobPodsLabel, null, null, null, null);
    } catch (ApiException e) {
      LOG.log(Level.SEVERE, "Exception when getting PodList.", e);
      throw new RuntimeException(e);
    }

    for (V1Pod pod : podList.getItems()) {
      LOG.info("a podIP in the job: " + pod.getStatus().getPodIP());
      if (podIP.equals(pod.getStatus().getPodIP())) {
        return pod.getStatus().getHostIP();
      }
    }

    return null;
  }

  /**
   * get the IP of the job master pod by using list method
   * @param namespace
   * @return
   */
  public static String getJobMasterIP(String namespace, String jobName) {

    if (apiClient == null || coreApi == null) {
      createApiInstances();
    }

    String jobMasterPodLabel = KubernetesUtils.createJobMasterServiceLabelWithKey(jobName);
    String jobMasterPodName = KubernetesUtils.createJobMasterPodName(jobName);

    V1PodList podList = null;
    try {
      podList = coreApi.listNamespacedPod(
          namespace, null, null, null, null, jobMasterPodLabel, null, null, null, null);
    } catch (ApiException e) {
      String logMessage = "Exception when getting the pod list: \n"
          + "exCode: " + e.getCode() + "\n"
          + "responseBody: " + e.getResponseBody();
      LOG.log(Level.SEVERE, logMessage, e);
      throw new RuntimeException(e);
    }

    for (V1Pod pod : podList.getItems()) {
      if (jobMasterPodName.equals(pod.getMetadata().getName())) {
        return pod.getStatus().getPodIP();
      }
    }

    return null;
  }

  /**
   * get the IP addresses of all pods in the job including the job master pod
   * this does not work if a pod is not initialized yet
   * in that case, only pod name is returned. pod ip is returned as null.
   * use watch api, instead of this list method.
   * @return
   */
  public static HashMap<String, String> getPodNamesAndIPsInJob(String namespace, String jobName) {

    if (apiClient == null || coreApi == null) {
      createApiInstances();
    }

    String jobPodsLabel = KubernetesUtils.createJobPodsLabelWithKey(jobName);
    HashMap<String, String> podNamesIPs = new HashMap<>();

    V1PodList podList = null;
    try {
      podList = coreApi.listNamespacedPod(
          namespace, null, null, null, null, jobPodsLabel, null, null, null, null);
    } catch (ApiException e) {
      String logMessage = "Exception when getting the pod list: \n"
          + "exCode: " + e.getCode() + "\n"
          + "responseBody: " + e.getResponseBody();
      LOG.log(Level.SEVERE, logMessage, e);
      throw new RuntimeException(e);
    }

    for (V1Pod pod : podList.getItems()) {
      String podName = pod.getMetadata().getName();
      String podIP = pod.getStatus().getPodIP();
      podNamesIPs.put(podName, podIP);
      LOG.info("Retrieved the pod address: " + podName + "[" + podIP + "]");
    }

    return podNamesIPs;
  }

  /**
   * a test method to see whether kubernetes java client can connect to kubernetes master
   * and get the pod list
   */
  public static void testGetPodList(String namespace) {
    if (apiClient == null || coreApi == null) {
      createApiInstances();
    }

    LOG.info("Getting the pod list for the namespace: " + namespace);
    V1PodList list = null;
    try {
      list = coreApi.listNamespacedPod(
          namespace, null, null, null, null, null, null, null, null, null);
    } catch (ApiException e) {
      String logMessage = "Exception when getting the pod list: \n"
          + "exCode: " + e.getCode() + "\n"
          + "responseBody: " + e.getResponseBody();
      LOG.log(Level.SEVERE, logMessage, e);
      throw new RuntimeException(e);
    }

    LOG.info("Number of pods in the received list: " + list.getItems().size());
    for (V1Pod item : list.getItems()) {
      LOG.info(item.getMetadata().getName());
    }
  }

  /**
   * test watch pods method in the worker pod
   */
  public static void testWatchPods(String namespace, String jobName, int timeout) {

    if (apiClient == null || coreApi == null) {
      createApiInstances();
    }

    String jobPodsLabel = KubernetesUtils.createJobPodsLabelWithKey(jobName);
    LOG.info("Starting the watcher for: " + namespace + ", " + jobName);
    Integer timeoutSeconds = timeout;
    Watch<V1Pod> watch = null;

    try {
      watch = Watch.createWatch(
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

    int eventCounter = 0;
    LOG.info("Getting watcher events.");

    for (Watch.Response<V1Pod> item : watch) {
      if (item.object != null) {
        LOG.info(eventCounter++ + "-Received watch event: "
            + item.object.getMetadata().getName() + ", "
            + item.object.getStatus().getPodIP() + ", "
            + item.object.getStatus().getPhase());
      } else {
        LOG.info("Received an event with item.object null.");
      }

      if (eventCounter == 5) {
        break;
      }
    }

    if (eventCounter != 5) {
      LOG.info("Has not received 5 events. Probably timeout limit has been reached.");
    }

    try {
      watch.close();
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Exception closing watcher.", e);
    }
  }

}
