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
package edu.iu.dsc.tws.comms.api;

import edu.iu.dsc.tws.comms.core.TaskPlan;

public interface DataFlowOperation {
  /**
   * Use this to inject partial results in a distributed dataflow operation
   * @param message message
   */
  boolean sendPartial(int source, Object message, int flags);

  /**
   * Send a send message, this call will work asynchronously
   * @param source
   * @param message
   */
  boolean send(int source, Object message, int flags);

  /**
   * Send the message on a specific path
   * @param source
   * @param message
   * @param dest
   * @return
   */
  boolean send(int source, Object message, int flags, int dest);

  /**
   * Send partial message on a specific path
   * @param source
   * @param message
   * @param dest
   * @return
   */
  boolean sendPartial(int source, Object message, int flags, int dest);

  /**
   * Progress the pending dataflow operations
   */
  void progress();

  /**
   * Clean up the resources
   */
  void close();

  /**
   * If this is a larger transfer of dataflow style, we need to finish
   */
  void finish();

  MessageType getType();

  TaskPlan getTaskPlan();

  /**
   * if set to true the operation will be memory mapped
   * @param memoryMapped
   */
  void setMemoryMapped(boolean memoryMapped);
}
