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
package edu.iu.dsc.tws.executor.comm;

import java.util.concurrent.BlockingQueue;

import edu.iu.dsc.tws.task.api.IMessage;

/**
 * Represents a parallel communication operation
 */
public interface IParallelOperation {
  /**
   * Send a message over the operation
   * @param source source
   * @param message the message
   */
  void send(int source, IMessage message);

  /**
   * Send a message to specific destination
   * @param source source
   * @param message message
   * @param dest destination
   */
  void send(int source, IMessage message, int dest);

  /**
   * Register a queue for receiving message
   * @param targetTask
   * @param queue
   */
  void register(int targetTask, BlockingQueue<IMessage> queue);

  /**
   * Progress the parallel operation
   */
  void progress();
}
