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
package edu.iu.dsc.tws.api.resource;

import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;

/**
 * Implementing component lets updating the status of a worker
 * It also lets retrieving the last status of all workers
 */
public interface IWorkerStatusUpdater {

  /**
   * update the status of this worker in the job
   * each worker reports its status changes with this method
   * Dashboard, JobMaster and other workers get worker status updates
   *
   * if status update is unsuccessful, return false
   * @param newState
   * @return
   */
  boolean updateWorkerStatus(JobMasterAPI.WorkerState newState);

  /**
   * return the status of any worker in the job
   * @param id
   * @return
   */
  JobMasterAPI.WorkerState getWorkerStatusForID(int id);


}
