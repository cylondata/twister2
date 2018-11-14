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
 * A class to transfer the job package to pods
 * static methods are used to start, cancel, signal and complete all transfer threads in the job
 * One thread is used to transfer the job package to one pod
 * File transfers are handled in parallel
 *
 * We either watch pods to become started and then start the file transfer attempts
 * or we don't watch pods. We just start the file transfer attempts after StatefulSets are created
 * If an attempt fails, we retry it after sleeping some time.
 * This behaviour is controlled by the configuration parameter in the config file:
 *   twister2.kubernetes.uploader.watch.pods.starting
 *
 * Determining pod starts:
 * when the first container in the pod becomes started, we assume the pod is started
 *
 * static methods:
 * Watcher gets events from previously finished pods also, if there were pods with the same name
 * We need to wait some time after starting watchers go ignore events from previously completed pods
 *   startTransferThreads is called immediately when KubernetesLauncher starts.
 *     While services and job master are started,
 *     watchers get events from previously finished pods with the same name if any.
 *   Then, setSubmittingStatefulSets method is called.
 *     we assume that events from previous pods have already been received upto this point.
 *     we get the first the Started event after this method is called
 *     and tell the transfer thread to start transfer attemps
 *   completeFileTransfers method is called, when StatefulSets are created.
 *     This method waits for all transfer threads to finish.
 *   cancelTrasfers: this method cancels file transfers
 *   if some thing goes wrong during job submission
 *
 */
public class JobPackageTransferThread extends Thread {
  private static final Logger LOG = Logger.getLogger(PodWatcher.class.getName());

  public static boolean watchBeforeUploading;

  public static final int MAX_WAIT_TIME_FOR_POD_START = 100; // in seconds
  public static final long SLEEP_INTERVAL_BETWEEN_TRANSFER_ATTEMPTS = 200;
  public static final long MAX_FILE_TRANSFER_TRY_COUNT = 100;

  private String namespace;
  private String podName;
  private String[] copyCommand;
  private String jobPackageFile;
  private boolean transferred = false;
  private static boolean cancelFileTransfer = false;
  private Watch<V1Event> watcher = null;

  private Object waitObject = new Object();

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

    if (watchBeforeUploading) {
      boolean podReady = watchPodToStarting();
      if (cancelFileTransfer) {
        return;
      }

      if (!podReady) {
        LOG.severe("Timeout limit has been reached. Pod has not started: " + podName);
        return;
      }
    } else {
      // wait for setSubmittingStatefulSets to be called
      synchronized (waitObject) {
        try {
          waitObject.wait();
        } catch (InterruptedException e) {
          LOG.warning("Thread wait interrupted.");
        }
      }
    }

    int tryCount = 0;

    while (!transferred && tryCount < MAX_FILE_TRANSFER_TRY_COUNT && !cancelFileTransfer) {
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

  private String copyCommandAsString() {
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
  private boolean watchPodToStarting() {

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

      if (cancelFileTransfer) {
        break;
      }

      if (item.object != null && reason.equals(item.object.getReason())) {
        i++;
      }

      if (item.object != null && reason.equals(item.object.getReason())
          && submittingStatefulSets) {
        podStarted = true;
        LOG.fine("Received Started event for the pod: " + podName + ", Started Count: " + i);
        break;
      }
    }

    try {
      watcher.close();
    } catch (IOException e) {
      LOG.log(Level.WARNING, "Exception when closing the watcher.", e);
    }

    return podStarted;
  }

  private void wakeupThread() {
    synchronized (waitObject) {
      waitObject.notify();
    }
  }

  /**
   * start job package transfer threads
   * @param namespace
   * @param job
   * @param jobPackageFile
   * @param watchBefore
   */
  public static void startTransferThreads(String namespace,
                                          JobAPI.Job job,
                                          String jobPackageFile,
                                          boolean watchBefore) {
    watchBeforeUploading = watchBefore;

    ArrayList<String> podNames = KubernetesUtils.generatePodNames(job);
    transferThreads = new JobPackageTransferThread[podNames.size()];

    for (int i = 0; i < podNames.size(); i++) {
      transferThreads[i] = new JobPackageTransferThread(namespace, podNames.get(i), jobPackageFile);
      transferThreads[i].start();
    }
  }

  /**
   * signal the threads that we are about to submit StatefulSet objects
   * only the Started events after this point will be considered valid
   */
  public static void setSubmittingStatefulSets() {
    submittingStatefulSets = true;
  }

  /**
   * wait for all transfer threads to finish
   * if any one of them fails, stop all active ones
   * @return
   */
  public static boolean completeFileTransfers() {

    // if pods are not watched, notify threads to start transfers
    if (!watchBeforeUploading) {
      // wakeup all threads and start transfer attempts, they are waiting for this call
      for (int i = 0; i < transferThreads.length; i++) {
        transferThreads[i].wakeupThread();
      }
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
    cancelFileTransfer = true;
  }

}
