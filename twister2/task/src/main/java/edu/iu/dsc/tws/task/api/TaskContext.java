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
package edu.iu.dsc.tws.task.api;

import java.util.Map;

/**
 * Task context
 */
public interface TaskContext {
  /**
   * Reset the context
   */
  void reset();

  /**
   * The task index
   * @return index
   */
  int taskIndex();

  /**
   * Task id
   * @return the task id
   */
  int taskId();

  /**
   * Name of the task
   */
  String taskName();

  /**
   * Get the parallism of the task
   * @return number of parallel instances
   */
  int getParallelism();

  /**
   * Get the worker id this task is running
   * @return worker id
   */
  int getWorkerId();

  /**
   * Get the task specific configurations
   * @return map of configurations
   */
  Map<String, Object> getConfigurations();

  /**
   * Get a configuration with a name
   *
   * @param name name of the config
   * @return the config, if not found return null
   */
  Object getConfig(String name);

  /**
   * Get the out edges of this task
   * @return the output edges
   */
  Map<String, String> getOutEdges();

  /**
   * Get the edge names and the tasks connected using those edges
   * @return a map with edge, Set<input task>
   */
  Map<String, String> getInputs();

  /**
   * Write a message with a key
   * @param edge the edge
   * @param key key
   * @param message message
   * @return true if the message is accepted
   */
  boolean write(String edge, Object key, Object message);

  /**
   * Write a message to the destination
   * @param edge edge
   * @param message message
   */
  boolean write(String edge, Object message);

  /**
   * Write the last message
   * @param edge edge
   * @param message message
   */
  boolean writeEnd(String edge, Object message);

  /**
   * Write the last message
   * @param edge edge
   * @param key key
   * @param message message
   */
  boolean writeEnd(String edge, Object key, Object message);

  /**
   * End the current writing
   * @param edge edge
   */
  void end(String edge);

  /**
   * Return true, if this task is done
   * @param edge edge name
   * @return boolean
   */
  boolean isDone(String edge);
}
