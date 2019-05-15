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
package edu.iu.dsc.tws.common.controller;

import java.util.List;

import edu.iu.dsc.tws.common.exceptions.TimeoutException;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;

/**
 * an interface to get the list of workers in a job and their addresses
 */
public interface IWorkerController {

  /**
   * return the WorkerInfo object for this worker
   * @return
   */
  JobMasterAPI.WorkerInfo getWorkerInfo();

  /**
   * return the WorkerInfo object for the given ID
   */
  JobMasterAPI.WorkerInfo getWorkerInfoForID(int id);

  /**
   * return the number of all workers in this job,
   * including non-started ones and finished ones
   * @return
   */
  int getNumberOfWorkers();

  /**
   * get all joined workers in this job, including the ones finished execution
   * some workers that has not joined yet, may not be included in this list.
   * users can compare the total number of workers to the size of this list and
   * understand whether there are non-joined workers
   * @return
   */
  List<JobMasterAPI.WorkerInfo> getJoinedWorkers();

  /**
   * get all workers in the job.
   * If some workers has not joined the job yet, wait for them.
   * After waiting for the timeout specified in ControllerContext.maxWaitTimeForAllToJoin
   * if some workers still could not join, throw an exception
   *
   * return all workers in the job including the ones that have already left, if any
   * @return
   */
  List<JobMasterAPI.WorkerInfo> getAllWorkers() throws TimeoutException;

  /**
   * wait for all workers in the job to arrive at this barrier
   * After waiting for the timeout specified in ControllerContext.maxWaitTimeOnBarrier
   * if some workers still could not arrive at the barrier, throw an exception
   * @return
   */
  void waitOnBarrier() throws TimeoutException;

  /**
   * Get object with a given name
   * @param name name
   * @return the object
   */
  default Object getRuntimeObject(String name) {
    return null;
  }
}
