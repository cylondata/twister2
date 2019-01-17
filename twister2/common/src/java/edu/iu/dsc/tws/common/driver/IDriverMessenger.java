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

import com.google.protobuf.Message;

/**
 * A messenger interface to send messages from the driver to the workers in a job
 */
public interface IDriverMessenger {

  /**
   * send a protocol buffer message to all workers in the job
   * @return
   */
  boolean broadcastToAllWorkers(Message message);

  /**
   * send a protocol buffer message to a list of workers in the job
   * @return
   */
  boolean sendToWorkerList(Message message, List<Integer> workerList);

  /**
   * send a protocol buffer message to a worker in the job
   * @return
   */
  boolean sendToWorker(Message message, Integer workerID);

}
