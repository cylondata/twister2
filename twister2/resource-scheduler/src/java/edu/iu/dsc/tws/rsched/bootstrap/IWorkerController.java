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
package edu.iu.dsc.tws.rsched.bootstrap;

import java.util.List;

/**
 * an interface to get the list of workers in a job and their addresses
 */
public interface IWorkerController {

  /**
   * return the WorkerInfo object for this worker
   * @return
   */
  WorkerInfo getWorkerInfo();

  /**
   * return the WorkerInfo object for the given ID
   */
  WorkerInfo getWorkerInfoForID(int id);

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
  List<WorkerInfo> getWorkerList();

  /**
   * wait for all workers to join the job
   * @param timeLimit
   * @return
   */
  List<WorkerInfo> waitForAllWorkersToJoin(long timeLimit);
}
