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
package edu.iu.dsc.tws.rsched.worker;

import java.util.List;

import edu.iu.dsc.tws.api.faulttolerance.JobProgress;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;

/**
 * this is the class that modifies JobProgress class
 */

public class JobProgressImpl extends JobProgress {

  public static void init() {
    jobStatus = JobStatus.STARTING;
    workerExecuteCount = 0;
  }

  public static void setJobStatus(JobStatus jobStatus1) {
    jobStatus = jobStatus1;
  }

  public static void increaseWorkerExecuteCount() {
    workerExecuteCount++;
  }

  public static void setRestartedWorkers(List<JobMasterAPI.WorkerInfo> restartedWorkers1) {
    restartedWorkers.clear();
    restartedWorkers.addAll(restartedWorkers1);
  }

}
