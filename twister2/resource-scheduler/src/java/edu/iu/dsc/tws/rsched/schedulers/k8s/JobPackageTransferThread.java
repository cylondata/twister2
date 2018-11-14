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
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.gson.reflect.TypeToken;

import edu.iu.dsc.tws.proto.system.job.JobAPI;

import io.kubernetes.client.ApiException;
import io.kubernetes.client.models.V1Event;
import io.kubernetes.client.util.Watch;

/**
 * a thread to transfer the job package to the assigned pod
 * it first waits for the pod to become ready.
 * when the first container in the pod becomes started, we assume the pod is ready
 * the events of the pods are watched by PodWatcher class
 * we only  check the status of the PodWatcher list from this thread
 */
public class JobPackageTransferThread extends Thread {
  private static final Logger LOG = Logger.getLogger(PodWatcher.class.getName());

  public static final int MAX_WAIT_TIME_FOR_POD_START = 100; // in seconds
  public static final long SLEEP_INTERVAL_BETWEEN_TRANSFER_ATTEMPTS = 200;
  public static final long MAX_FILE_TRANSFER_TRY_COUNT = 100;

  private String namespace;
  private String podName;
  private String[] copyCommand;
  private String jobPackageFile;
  private boolean transferred = false;
  private static boolean stopExecution = false;
  private Watch<V1Event> watcher = null;

  private static JobPackageTransferThread[] transferThreads;
  private static boolean submittingStatefulSets = false;

  public JobPackageTransferThread(String namespace, String podName, String jobPackageFile) {
    this.namespace = namespace;
    this.jobPackageFile = jobPackageFile;
    this.podName = podName;
    copyCommand = KubernetesUtils.createCopyCommand(jobPackageFile, namespace, podName);
  }

  /**
   * return true if the file is transferred successfully
   * @return
   */
  public boolean packageTransferred() {
    return transferred;
  }

  /**
   * pod name for this thread
   * @return
   */
  public String getPodName() {
    return podName;
  }

  @Override
  public void run() {

    boolean podReady = watchPodToStarting();

    if (!podReady) {
      LOG.severe("Timeout limit has been reached. Pod has not started: " + podName);
      return;
    }

    int tryCount = 0;

    while (!transferred && tryCount < MAX_FILE_TRANSFER_TRY_COUNT && !stopExecution) {
      transferred = KubernetesController.runProcess(copyCommand);
      if (transferred) {
        LOG.info("Job Package: " + jobPackageFile + " transferred to the pod: " + podName);

      } else {

        tryCount++;

        if (tryCount == 10 || tryCount == (MAX_FILE_TRANSFER_TRY_COUNT - 1)) {
          LOG.warning("Job Package: " + jobPackageFile + " could not be transferred to "
              + "the pod: " + podName + ". Sleeping and will try again ... " + tryCount
              + "\nExecuted command: " + copyCommandAsString());
        }

        try {
          Thread.sleep(SLEEP_INTERVAL_BETWEEN_TRANSFER_ATTEMPTS);
        } catch (InterruptedException e) {
          LOG.log(Level.WARNING, "Thread sleep interrupted.", e);
        }
      }
    }

  }

  public String copyCommandAsString() {
    String copyStr = "";
    for (String cmd: copyCommand) {
      copyStr += cmd + " ";
    }
    return copyStr;
  }

  /**
   * watch all pods in the given list until they become Starting
   * flag the pods with a true value in the given HashMap
   */
  public boolean watchPodToStarting() {

    /** Event Reasons: SuccessfulMountVolume, Killing, Scheduled, Pulled, Created, Started
     * ref: https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-phase */

    if (PodWatchUtils.apiClient == null || PodWatchUtils.coreApi == null) {
      PodWatchUtils.createApiInstances();
    }

    String fieldSelector = "involvedObject.name=" + podName;
    String reason = "Started";

    try {
      watcher = Watch.createWatch(
          PodWatchUtils.apiClient,
          PodWatchUtils.coreApi.listNamespacedEventCall(namespace, null, null, fieldSelector,
              null, null, null, null, MAX_WAIT_TIME_FOR_POD_START, Boolean.TRUE, null, null),
          new TypeToken<Watch.Response<V1Event>>() {
          }.getType());

    } catch (ApiException e) {
      LOG.log(Level.SEVERE, "Can not start event watcher for the namespace: " + namespace, e);
      return false;
    }

    boolean podStarted = false;

    int i = 0;
    for (Watch.Response<V1Event> item : watcher) {

      if (item.object != null && reason.equals(item.object.getReason())) {
        i++;
      }

      if (item.object != null && reason.equals(item.object.getReason())
          && JobPackageTransferThread.statefulSetsSubmitting()) {
        podStarted = true;
        LOG.info("Received Started event for the pod: " + podName + ", Started Count: " + i);
        break;
      }
    }

    try {
      watcher.close();
    } catch (IOException e) {
      LOG.log(Level.WARNING, "Exception when clsoing the watcher.", e);
    }

    return podStarted;
  }

  public void cancelTransfer() {

    if (watcher == null) {
      return;
    }

    try {
      watcher.close();
    } catch (IOException e) {
      LOG.log(Level.WARNING, "Exception when clsoing the watcher.", e);
    }

  }

  public static void setStopExecution() {
    stopExecution = true;
  }

  public static void setSubmittingStatefulSets() {
    submittingStatefulSets = true;
  }

  public static boolean statefulSetsSubmitting() {
    return submittingStatefulSets;
  }

  public static void startTransferThreads(String namespace, JobAPI.Job job, String jobPackageFile) {
    ArrayList<String> podNames = KubernetesUtils.generatePodNames(job);
    transferThreads = new JobPackageTransferThread[podNames.size()];

    for (int i = 0; i < podNames.size(); i++) {
      transferThreads[i] =
          new JobPackageTransferThread(namespace, podNames.get(i), jobPackageFile);

      transferThreads[i].start();
    }

  }

  public static boolean finishTransferThreads() {
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
        LOG.log(Level.WARNING, "Thread sleep interrupted.", e);
      }
    }

    // if one transfer fails, tell all transfer threads to stop and return false
    if (!allTransferred) {
      cancelTransfers();
    }

    return allTransferred;

  }

  public static void cancelTransfers() {
    setStopExecution();

    if (transferThreads == null) {
      return;
    }

    for (int i = 0; i < transferThreads.length; i++) {
      transferThreads[i].cancelTransfer();
    }
  }


}
