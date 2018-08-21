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
package edu.iu.dsc.tws.common.discovery;

import java.util.List;

/**
 * an interface to get the list of workers in a job and their addresses
 */
public interface IWorkerController {

  /**
   * return the WorkerNetworkInfo object for this worker
   * @return
   */
  WorkerNetworkInfo getWorkerNetworkInfo();

  /**
   * return the WorkerNetworkInfo object for the given ID
   */
  WorkerNetworkInfo getWorkerNetworkInfoForID(int id);

  /**
   * return the number of all workers in this job,
   * including non-started ones and finished ones
   * @return
   */
  int getNumberOfWorkers();

  /**
   * return all known workers in a job, including the ones finished execution
   * some workers that has not joined yet, may not be included in this list.
   * users can compare the total number of workers to the size of this list and
   * understand whether there are non-joined workers
   * @return
   */
  List<WorkerNetworkInfo> getWorkerList();

  /**
   * wait for all workers to join the job
   * return all workers in the job including the ones that have already left, if any
   * @param timeLimitMilliSec
   * @return
   */
  List<WorkerNetworkInfo> waitForAllWorkersToJoin(long timeLimitMilliSec);

  /**
   * wait for all workers in the job to arrive at this barrier
   * if the time limit has been reached before all workers arrived, return false
   * otherwise return true
   * @param timeLimitMilliSec
   * @return
   */
  boolean waitOnBarrier(long timeLimitMilliSec);
}
