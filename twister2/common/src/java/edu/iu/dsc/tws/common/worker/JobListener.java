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
package edu.iu.dsc.tws.common.worker;

import java.util.List;

import com.google.protobuf.Any;

import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;

/**
 * IWorker should listen on this interface to get messages from the driver
 */
public interface JobListener {

  /**
   * called when new instances of workers are added the job
   * @param instancesAdded
   */
  void workersScaledUp(int instancesAdded);

  /**
   * called when new instances of workers are removed from the job
   * @param instancesRemoved
   */
  void workersScaledDown(int instancesRemoved);

  /**
   * received a broadcast message from the driver
   * @param anyMessage received message from the driver
   */
  void broadcastReceived(Any anyMessage);

  /**
   * this method is invoked when all workers joined the job initially
   * and also, after each scale up operation,
   * when all new workers joined the job, it is invoked
   * @param workerList
   */
  void allWorkersJoined(List<JobMasterAPI.WorkerInfo> workerList);

}
