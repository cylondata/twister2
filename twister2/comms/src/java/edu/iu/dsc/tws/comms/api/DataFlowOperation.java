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

/**
 * The communication operation interface.
 */
public interface DataFlowOperation {
  /**
   * Use this to inject partial results in a distributed dataflow operation
   *
   * @param message message
   * @return true if message is accepted
   */
  boolean sendPartial(int source, Object message, int flags);

  /**
   * Send a send message, this call will work asynchronously
   *
   * @param source source task
   * @param message message as a generic object
   * @return true if message is accepted
   */
  boolean send(int source, Object message, int flags);

  /**
   * Send the message on a specific path
   *
   * @param source source task
   * @param message a generic java object
   * @param target target task
   * @return true if message is accepted
   */
  boolean send(int source, Object message, int flags, int target);

  /**
   * Send partial message on a specific path
   *
   * @param source the source
   * @param message message as a generic object
   * @param target the final target
   * @return true if message is accepted
   */
  boolean sendPartial(int source, Object message, int flags, int target);

  /**
   * Progress the pending dataflow operations
   *
   * @return true if there is more messages to communicationProgress, unless return false
   */
  boolean progress();

  /**
   * Clean up the resources
   */
  void close();

  /**
   * Weather the operation doesn't have any pending sends or receives
   *
   * @return is complete
   */
  default boolean isComplete() {
    return false;
  }

  default boolean isDelegeteComplete() {
    return false;
  }

  /**
   * If this is a larger transfer of dataflow style, we need to finish
   */
  default void finish(int source) {
  }

  /**
   * returns the key type that is associated with the data flow operation
   *
   * @return the MessageType or an UnsupportedOperationException
   */
  default MessageType getKeyType() {
    throw new UnsupportedOperationException("method not supported");
  }

  /**
   * returns the data type that is associated with the data flow operation
   *
   * @return the MessageType or an UnsupportedOperationException
   */
  default MessageType getDataType() {
    throw new UnsupportedOperationException("method not supported");
  }

  /**
   * Task plan associated with this operation
   *
   * @return task plan
   */
  TaskPlan getTaskPlan();
}
