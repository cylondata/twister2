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

import java.io.File;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.gson.reflect.TypeToken;
import com.squareup.okhttp.Response;

import edu.iu.dsc.tws.rsched.utils.ProcessUtils;

import io.kubernetes.client.ApiClient;
import io.kubernetes.client.ApiException;
import io.kubernetes.client.Configuration;
import io.kubernetes.client.apis.AppsV1beta2Api;
import io.kubernetes.client.apis.CoreV1Api;
import io.kubernetes.client.models.V1DeleteOptions;
import io.kubernetes.client.models.V1Event;
import io.kubernetes.client.models.V1Pod;
import io.kubernetes.client.models.V1Service;
import io.kubernetes.client.models.V1ServiceList;
import io.kubernetes.client.models.V1beta2StatefulSet;
import io.kubernetes.client.models.V1beta2StatefulSetList;
import io.kubernetes.client.util.Watch;

public class KubernetesController {
  private static final Logger LOG = Logger.getLogger(KubernetesController.class.getName());

  private ApiClient client = null;
  private CoreV1Api coreApi;
  private AppsV1beta2Api beta2Api;

  public void init() {
    createApiInstances();
  }

  public void createApiInstances() {
    try {
      client = io.kubernetes.client.util.Config.defaultClient();
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Exception when creating ApiClient: ", e);
      throw new RuntimeException(e);
    }
    Configuration.setDefaultApiClient(client);

    coreApi = new CoreV1Api();
    beta2Api = new AppsV1beta2Api(client);
  }

  /**
   * return the StatefulSet object if it exists in the Kubernetes master,
   * otherwise return null
   */
  public V1beta2StatefulSet getStatefulSet(String namespace, String statefulSetName) {

    V1beta2StatefulSetList setList = null;
    try {
      setList = beta2Api.listNamespacedStatefulSet(
          namespace, null, null, null, null, null, null, null, null, null);
    } catch (ApiException e) {
      LOG.log(Level.SEVERE, "Exception when getting StatefulSet list.", e);
      throw new RuntimeException(e);
    }

    for (V1beta2StatefulSet statefulSet : setList.getItems()) {
      if (statefulSetName.equals(statefulSet.getMetadata().getName())) {
        return statefulSet;
      }
    }

    return null;
  }

  /**
   * create the given service on Kubernetes master
   */
  public boolean createStatefulSetJob(String namespace, V1beta2StatefulSet statefulSet) {

    String statefulSetName = statefulSet.getMetadata().getName();
    try {
      Response response = beta2Api.createNamespacedStatefulSetCall(
          namespace, statefulSet, null, null, null).execute();

      if (response.isSuccessful()) {
        LOG.log(Level.INFO, "StatefulSet [" + statefulSetName
            + "] is created for the same named job.");
        return true;

      } else {
        LOG.log(Level.SEVERE, "Error when creating the StatefulSet [" + statefulSetName + "]: "
            + response);
        LOG.log(Level.SEVERE, "Submitted StatefulSet Object: " + statefulSet);
        return false;
      }

    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Exception when creating the StatefulSet: " + statefulSetName, e);
    } catch (ApiException e) {
      LOG.log(Level.SEVERE, "Exception when creating the StatefulSet: " + statefulSetName, e);
    }
    return false;
  }

  /**
   * delete the given StatefulSet from Kubernetes master
   */
  public boolean deleteStatefulSetJob(String namespace, String statefulSetName) {

    try {
      V1DeleteOptions deleteOptions = new V1DeleteOptions();
      deleteOptions.setGracePeriodSeconds(0L);
      deleteOptions.setPropagationPolicy(KubernetesConstants.DELETE_OPTIONS_PROPAGATION_POLICY);

      Response response = beta2Api.deleteNamespacedStatefulSetCall(
          statefulSetName, namespace, deleteOptions, null, null, null, null, null, null).execute();

      if (response.isSuccessful()) {
        LOG.log(Level.INFO, "StatefulSet for the Job [" + statefulSetName + "] is deleted.");
        return true;

      } else {

        if (response.code() == 404 && response.message().equals("Not Found")) {
          LOG.log(Level.SEVERE, "There is no StatefulSet for the Job [" + statefulSetName
              + "] to delete on Kubernetes master. It may have already terminated.");
          return true;
        }

        LOG.log(Level.SEVERE, "Error when deleting the StatefulSet of the job ["
            + statefulSetName + "]: " + response);
        return false;
      }

    } catch (ApiException e) {
      LOG.log(Level.SEVERE, "Exception when deleting the the StatefulSet of the job: "
          + statefulSetName, e);
      return false;
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Exception when deleting the the StatefulSet of the job: "
          + statefulSetName, e);
      return false;
    }
  }


  /**
   * create the given service on Kubernetes master
   */
  public boolean createService(String namespace, V1Service service) {

    String serviceName = service.getMetadata().getName();
    try {
      Response response = coreApi.createNamespacedServiceCall(
          namespace, service, null, null, null).execute();

      if (response.isSuccessful()) {
        LOG.log(Level.INFO, "Service [" + serviceName + "] created.");
        return true;
      } else {
        LOG.log(Level.SEVERE, "Error when creating the service [" + serviceName + "]: " + response);
        return false;
      }

    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Exception when creating the service: " + serviceName, e);
    } catch (ApiException e) {
      LOG.log(Level.SEVERE, "Exception when creating the service: " + serviceName, e);
    }
    return false;
  }

  /**
   * return the service object if it exists in the Kubernetes master,
   * otherwise return null
   */
  public V1Service getService(String namespace, String serviceName) {

    V1ServiceList serviceList = null;
    try {
      serviceList = coreApi.listNamespacedService(namespace,
          null, null, null, null, null, null, null, null, null);
    } catch (ApiException e) {
      LOG.log(Level.SEVERE, "Exception when getting service list.", e);
      throw new RuntimeException(e);
    }

    for (V1Service service : serviceList.getItems()) {
      if (serviceName.equals(service.getMetadata().getName())) {
        return service;
      }
    }

    return null;
  }

  /**
   * delete the given service from Kubernetes master
   */
  public boolean deleteService(String namespace, String serviceName) {

    try {
      Response response = coreApi.deleteNamespacedServiceCall(
          serviceName, namespace, null, null, null).execute();

      if (response.isSuccessful()) {
        LOG.log(Level.INFO, "Service [" + serviceName + "] is deleted.");
        return true;

      } else {

        if (response.code() == 404 && response.message().equals("Not Found")) {
          LOG.log(Level.SEVERE, "There is no Service [" + serviceName
              + "] to delete on Kubernetes master. It may have already been terminated.");
          return true;
        }

        LOG.log(Level.SEVERE, "Error when deleting the Service [" + serviceName + "]: " + response);
        return false;
      }
    } catch (ApiException e) {
      LOG.log(Level.SEVERE, "Exception when deleting the service: " + serviceName, e);
      return false;
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Exception when deleting the the service: " + serviceName, e);
      return false;
    }
  }

  /**
   * transfer the job package to pods in parallel by many threads
   * @param namespace
   * @param jobName
   * @param numberOfPods
   * @param jobPackageFile
   * @return
   */
  public boolean transferJobPackageInParallel(String namespace, String jobName, int numberOfPods,
                                              String jobPackageFile) {

    PodWatcher podWatcher = new PodWatcher(namespace, jobName, numberOfPods, client, coreApi);
    podWatcher.start();

//    try {
//      watcher.join();
//    } catch (InterruptedException e) {
//      e.printStackTrace();
//    }

    JobPackageTransferThread[] transferThreads = new JobPackageTransferThread[numberOfPods];
    for (int i = 0; i < numberOfPods; i++) {
      transferThreads[i] =
          new JobPackageTransferThread(namespace, jobName, i, jobPackageFile, podWatcher);

      transferThreads[i].start();
    }

    // wait all transfer threads to finish up
    boolean allTransferred = true;
    for (int i = 0; i < transferThreads.length; i++) {
      try {
        transferThreads[i].join();
        if (!transferThreads[i].packageTransferred()) {
          LOG.log(Level.SEVERE, "Job Package is not transferred to the pod: "
              + transferThreads[i].getPodName());
          allTransferred = false;
          break;
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    // if one transfer fails, stop all transfer threads and return false
    if (!allTransferred) {
      for (int i = 0; i < transferThreads.length; i++) {
        transferThreads[i].setStopExecution();
      }
    }

    return allTransferred;
  }

  /**
   * watch events until getting a container started event in the given pod
   * does not matter which container started
   */
  public boolean waitUntilAContainerStarts(String namespace, String podName) {

    /** Event Reasons: SuccessfulMountVolume, Killing, Scheduled, Pulled, Created, Started
     * ref: https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-phase */

    String reason = "Started";
    Watch<V1Event> watch = null;
    try {
      watch = Watch.createWatch(
          client,
          coreApi.listNamespacedEventCall(
              namespace, null, null, null, null, null, 10, null, null, Boolean.TRUE, null, null),
          new TypeToken<Watch.Response<V1Event>>() {
          }.getType());

    } catch (ApiException e) {
      LOG.log(Level.SEVERE, "Can not start event watcher for the namespace: " + namespace, e);
      return false;
    }

    boolean result = false;

    for (Watch.Response<V1Event> item : watch) {
      if (item.object != null) {

        if (podName.equals(item.object.getInvolvedObject().getName())
            && reason.equals(item.object.getReason())) {
          result = true;
          LOG.log(Level.INFO, "Container started event received for the pod: " + podName);
          break;
        }
      }
    }

    try {
      watch.close();
    } catch (IOException e) {
      e.printStackTrace();
    }

    return result;
  }

  /**
   * watch events until getting the Running event for the given pod
   */
  public boolean waitUntilPodRunning(String namespace, String podName) {

    /** Pod Phases: Pending, Running, Succeeded, Failed, Unknown
     * ref: https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-phase */

    String phase = "Running";
    Watch<V1Pod> watch = null;
    try {
      watch = Watch.createWatch(
          client,
          coreApi.listNamespacedPodCall(
              namespace, null, null, null, null, null, 10, null, null, Boolean.TRUE, null, null),
          new TypeToken<Watch.Response<V1Pod>>() {
          }.getType());

    } catch (ApiException e) {
      e.printStackTrace();
    }

    boolean result = false;

    for (Watch.Response<V1Pod> item : watch) {
      if (item.object != null
          && podName.equals(item.object.getMetadata().getName())
          && phase.equals(item.object.getStatus().getPhase())) {
        LOG.log(Level.INFO, "Received pod Running event for the pod: " + podName);
        result = true;
        break;
      }
    }

    try {
      watch.close();
    } catch (IOException e) {
      e.printStackTrace();
    }

    return result;
  }

  /**
   * transfer the job package to all pods in a job sequentially
   */
  public boolean transferJobPackageSequentially(String namespace, String jobName, int numberOfPods,
                                                String jobPackageFile) {

    // wait until a container is started in the first pod
    String firstPod = KubernetesUtils.podNameFromJobName(jobName, 0);
    boolean containerStarted = waitUntilAContainerStarts(namespace, firstPod);
    if (!containerStarted) {
      return false;
    }

    for (int i = 0; i < numberOfPods; i++) {
      String podName = KubernetesUtils.podNameFromJobName(jobName, i);
      String[] copyCommand = KubernetesUtils.createCopyCommand(jobPackageFile, namespace, podName);
      boolean transferred = transferJobPackageToAPod(copyCommand, podName, jobPackageFile);

      if (!transferred) {
        return false;
      }
    }

    return true;
  }

  /**
   * transfer the job package to a pod with the given command
   * it uses "kubectl cp" command to transfer the file to the file
   */
  public boolean transferJobPackageToAPod(String[] copyCommand, String podName,
                                          String jobPackageFile) {

    int maxTryCount = 5;
    boolean transferred = false;
    int tryCount = 0;

    while (!transferred && tryCount < maxTryCount) {
      transferred = runProcess(copyCommand);
      if (transferred) {
        LOG.log(Level.INFO, "Job Package: " + jobPackageFile
            + " transferred to the pod: " + podName);
        return true;
      } else {
        try {
          LOG.log(Level.WARNING, "Job Package: " + jobPackageFile + " could not be transferred to "
              + "the pod: " + podName + ". Sleeping and will try again ... " + tryCount++);

          Thread.sleep(100);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }

    return false;
  }

  /**
   * sending a command to shell
   */
  public static boolean runProcess(String[] command) {
    StringBuilder stdout = new StringBuilder();
    StringBuilder stderr = new StringBuilder();
    int status =
        ProcessUtils.runSyncProcess(false, command, stderr, new File("."), false);

    if (status != 0) {
      LOG.severe(String.format(
          "Failed to run process. Command=%s, STDOUT=%s, STDERR=%s", command, stdout, stderr));
    }
    return status == 0;
  }

}
