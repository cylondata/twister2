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
package edu.iu.dsc.tws.common.driver;

import java.util.List;

import com.google.protobuf.Any;

import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;

public interface DriverJobListener {

  /**
   * received a protocol buffer message from a worker
   * @param anyMessage received message from the worker
   */
  void workerMessageReceived(Any anyMessage, int senderWorkerID);

  /**
   * this method is invoked when all workers joined the job initially
   * and also, after each scale up operation,
   * when all new workers joined the job, it is invoked
   * @param workerList
   */
  void allWorkersJoined(List<JobMasterAPI.WorkerInfo> workerList);

}
