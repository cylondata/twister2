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

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;

/**
 * This class is used to monitor the status of the running job
 * Extended class modifies members
 */
@SuppressWarnings("HideUtilityClassConstructor")
public class JobProgress {

  private static final Logger LOG = Logger.getLogger(JobProgress.class.getName());

  /**
   * Job state
   */
  public enum JobStatus {
    STARTING,
    RESTARTING,
    EXECUTING,
    FAULTY
  }

  protected static JobStatus jobStatus;

  protected static int workerExecuteCount;

  protected static List<JobMasterAPI.WorkerInfo> restartedWorkers = new LinkedList<>();

  /**
   * Keep track of the components that have the ability to deal with faults
   */
  protected static List<FaultAcceptable> faultAcceptors = new ArrayList<>();


  public JobProgress() {
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

  public static List<JobMasterAPI.WorkerInfo> getRestartedWorkers() {
    return restartedWorkers;
  }

  public static void registerFaultAcceptor(FaultAcceptable faultAcceptable) {
    LOG.info("registered FaultAcceptable");
    faultAcceptors.add(faultAcceptable);
  }

  public static void unRegisterFaultAcceptor(FaultAcceptable faultAcceptable) {
    faultAcceptors.remove(faultAcceptable);
  }

}
