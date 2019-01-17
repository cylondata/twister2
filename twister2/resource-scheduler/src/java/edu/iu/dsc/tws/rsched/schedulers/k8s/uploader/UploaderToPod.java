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

import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesController;
import edu.iu.dsc.tws.rsched.schedulers.k8s.KubernetesUtils;

/**
 * upload the job package to a worker
 * it assumes the target pod is ready
 * so MAX_FILE_TRANSFER_TRY_COUNT is set to only a few
 */

public class UploaderToPod extends Thread {
  private static final Logger LOG = Logger.getLogger(UploaderToPod.class.getName());

  public static final long MAX_FILE_TRANSFER_TRY_COUNT = 3;
  public static final long SLEEP_INTERVAL_BETWEEN_TRANSFER_ATTEMPTS = 200;

  private String namespace;
  private String podName;
  private String jobPackageFile;

  private boolean transferred = false;
  private boolean cancelFileTransfer = false;

  public UploaderToPod(String namespace, String podName, String jobPackageFile) {
    this.namespace = namespace;
    this.podName = podName;
    this.jobPackageFile = jobPackageFile;
  }

  /**
   * return true if the file is transferred successfully
   * @return
   */
  public boolean packageTransferred() {
    return transferred;
  }

  public void cancelTransfer() {
    cancelFileTransfer = true;
  }

  @Override
  public void run() {
    // generate copy command
    String[] copyCommand = KubernetesUtils.createCopyCommand(jobPackageFile, namespace, podName);

    // count transfer attempts
    int attemptCount = 0;

    while (!transferred && attemptCount < MAX_FILE_TRANSFER_TRY_COUNT && !cancelFileTransfer) {
      transferred = KubernetesController.runProcess(copyCommand);
      if (transferred) {
        LOG.info("Job Package: " + jobPackageFile + " transferred to the pod: " + podName);

      } else {

        attemptCount++;

        if (attemptCount == MAX_FILE_TRANSFER_TRY_COUNT) {
          LOG.warning("Job Package: " + jobPackageFile + " could not be transferred to "
              + "the pod: " + podName + ". Attempt count: " + attemptCount
              + "\nExecuted command: " + copyCommandAsString(copyCommand));
        } else {

          try {
            Thread.sleep(SLEEP_INTERVAL_BETWEEN_TRANSFER_ATTEMPTS);
          } catch (InterruptedException e) {
            LOG.log(Level.WARNING, "Thread sleep interrupted.", e);
          }
        }
      }
    }
  }

  private String copyCommandAsString(String[] copyCommand) {
    String copyStr = "";
    for (String cmd: copyCommand) {
      copyStr += cmd + " ";
    }
    return copyStr;
  }

}
