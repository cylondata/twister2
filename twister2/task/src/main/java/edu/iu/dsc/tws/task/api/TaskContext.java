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

import java.util.HashMap;
import java.util.Map;

/**
 * Task context
 */
public class TaskContext {
  /**
   * Task index, which goes from 0 up to the number of parallel tasks
   */
  private int taskIndex;

  /**
   * Unique id of the task
   */
  private int taskId;

  /**
   * Name of the task
   */
  private String taskName;

  /**
   * Parallel instances of the task
   */
  private int parallelism;

  /**
   * Collect output
   */
  private OutputCollection collection;

  /**
   * Task specific configurations
   */
  private Map<String, Object> configs;

  /**
   * The worker id this task instance belongs to
   */
  private int workerId;

  /**
   * Keep track of the edges that are been done
   */
  private Map<String, Boolean> isDone = new HashMap<>();

  public TaskContext(int taskIndex, int taskId, String taskName,
                     int parallelism, int wId, Map<String, Object> configs) {
    this.taskIndex = taskIndex;
    this.taskId = taskId;
    this.taskName = taskName;
    this.parallelism = parallelism;
    this.configs = configs;
    this.workerId = wId;
  }

  public TaskContext(int taskIndex, int taskId, String taskName, int parallelism, int wId,
                     OutputCollection collection, Map<String, Object> configs) {
    this.taskIndex = taskIndex;
    this.taskId = taskId;
    this.taskName = taskName;
    this.parallelism = parallelism;
    this.collection = collection;
    this.configs = configs;
    this.workerId = wId;
  }

  /**
   * Reset the context
   */
  public void reset() {
    this.isDone = new HashMap<>();
  }

  /**
   * The task index
   * @return index
   */
  public int taskIndex() {
    return taskIndex;
  }

  /**
   * Task id
   * @return the task id
   */
  public int taskId() {
    return taskId;
  }

  /**
   * Name of the task
   */
  public String taskName() {
    return taskName;
  }

  /**
   * Get the parallism of the task
   * @return number of parallel instances
   */
  public int getParallelism() {
    return parallelism;
  }

  /**
   * Get the worker id this task is running
   * @return worker id
   */
  public int getWorkerId() {
    return workerId;
  }

  /**
   * Get the task specific configurations
   * @return map of configurations
   */
  public Map<String, Object> getConfigurations() {
    return configs;
  }

  /**
   * Write a message with a key
   * @param edge the edge
   * @param key key
   * @param message message
   * @return true if the message is accepted
   */
  public boolean write(String edge, Object key, Object message) {
    if (isDone.containsKey(edge) && isDone.get(edge)) {
      throw new RuntimeException("Cannot send on a stream that ended");
    }
    return collection.collect(edge, new TaskMessage(key, message, edge, taskId));
  }

  /**
   * Write a message to the destination
   * @param edge edge
   * @param message message
   */
  public boolean write(String edge, Object message) {
    if (isDone.containsKey(edge) && isDone.get(edge)) {
      throw new RuntimeException("Cannot send on a stream that ended");
    }
    return collection.collect(edge, new TaskMessage(message, edge, taskId));
  }

  /**
   * Write the last message
   * @param edge edge
   * @param message message
   */
  public boolean writeEnd(String edge, Object message) {
    if (isDone.containsKey(edge) && isDone.get(edge)) {
      throw new RuntimeException("Cannot send on a stream that ended");
    }
    boolean collect = collection.collect(edge, new TaskMessage(message, edge, taskId));
    isDone.put(edge, true);
    return collect;
  }

  /**
   * Write the last message
   * @param edge edge
   * @param key key
   * @param message message
   */
  public boolean writeEnd(String edge, Object key, Object message) {
    if (isDone.containsKey(edge) && isDone.get(edge)) {
      throw new RuntimeException("Cannot send on a stream that ended");
    }
    boolean collect = collection.collect(edge, new TaskMessage(key, message, edge, taskId));
    isDone.put(edge, true);
    return collect;
  }

  /**
   * End the current writing
   * @param edge edge
   */
  public void end(String edge) {
    isDone.put(edge, true);
  }

  /**
   * Return true, if this task is done
   * @param edge edge name
   * @return boolean
   */
  public boolean isDone(String edge) {
    return isDone.containsKey(edge) && isDone.get(edge);
  }
}
