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

import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * a thread to transfer the job package to the assigned pod
 * it first waits for the pod to become ready.
 * when the first container in the pod becomes started, we assume the pod is ready
 * the events of the pods are watched by PodWatcher class
 * we only  check the status of the PodWatcher list from this thread
 */
public class JobPackageTransferThread extends Thread {
  private static final Logger LOG = Logger.getLogger(PodWatcher.class.getName());

  public static final long SLEEP_INTERVAL_BETWEEN_POD_STATUS_CHECKS = 30;
  public static final long SLEEP_INTERVAL_BETWEEN_TRANSFER_ATTEMPTS = 200;
  public static final long MAX_FILE_TRANSFER_TRY_COUNT = 50;

  private String podName;
  private String[] copyCommand;
  private String jobPackageFile;
  private HashMap<String, Boolean> pods;
  private boolean transferred = false;
  private boolean stopExecution = false;

  public JobPackageTransferThread(String namespace, String jobName, int podIndex,
                                   String jobPackageFile, PodWatcher podWatcher) {

    this.jobPackageFile = jobPackageFile;
    podName = KubernetesUtils.podNameFromJobName(jobName, podIndex);
    copyCommand = KubernetesUtils.createCopyCommand(jobPackageFile, namespace, podName);
    pods = podWatcher.getPods();
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

  public void setStopExecution() {
    stopExecution = true;
  }


  @Override
  public void run() {

    boolean podReady = pods.get(podName);

    // wait until the pod is ready for file transfer
    while (!podReady && !stopExecution) {
      try {
        Thread.sleep(SLEEP_INTERVAL_BETWEEN_POD_STATUS_CHECKS);
      } catch (InterruptedException e) {
        LOG.log(Level.WARNING, "Thread sleep interrupted.", e);
      }

      podReady = pods.get(podName);
      if (podReady) {
        LOG.log(Level.INFO, "Pod is ready to transfer. Starting to transfer the file to the pod: "
            + podName);
      }
    }

    int tryCount = 0;

    while (!transferred && tryCount < MAX_FILE_TRANSFER_TRY_COUNT && !stopExecution) {
      transferred = KubernetesController.runProcess(copyCommand);
      if (transferred) {
        LOG.info("Job Package: " + jobPackageFile + " transferred to the pod: " + podName);

      } else {

        tryCount++;

        if (tryCount == 5 || tryCount == (MAX_FILE_TRANSFER_TRY_COUNT - 1)) {
          LOG.warning("Job Package: " + jobPackageFile + " could not be transferred to "
              + "the pod: " + podName + ". Sleeping and will try again ... " + tryCount
              + "\nFailed command: " + copyCommandAsString());
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
}
