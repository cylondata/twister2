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
package edu.iu.dsc.tws.api.faulttolerance;

/**
 * This class is used to monitor the status of the running job
 */
public final class Progress {

  /**
   * Job state
   */
  public enum JobStatus {
    STARTING,
    RESTARTING,
    EXECUTING,
    FAULTY
  }

  private static JobStatus jobStatus;

  private static int workerExecuteCount;

  private Progress() {
  }

  public static void init() {
    jobStatus = JobStatus.STARTING;
    workerExecuteCount = 0;
  }

  public static void setJobStatus(JobStatus jobStatus1) {
    jobStatus = jobStatus1;
  }

  public static JobStatus getJobStatus() {
    return jobStatus;
  }

  public static boolean isJobHealthy() {
    return jobStatus != JobStatus.FAULTY;
  }

  public static boolean isJobFaulty() {
    return jobStatus == JobStatus.FAULTY;
  }

  public static int getWorkerExecuteCount() {
    return workerExecuteCount;
  }

  public static void increaseWorkerExecuteCount() {
    workerExecuteCount++;
  }
}
