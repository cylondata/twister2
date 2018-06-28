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

public class TaskContext {
  private int taskIndex;

  private int taskId;

  private String taskName;

  private int parallelism;

  private OutputCollection collection;

  private Map<String, Object> configs;

  private int workerId;

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
   * The task index
   * @return
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

  public int getParallelism() {
    return parallelism;
  }

  public int getWorkerId() {
    return workerId;
  }

  public Map<String, Object> getConfigurations() {
    return configs;
  }

  /**
   * Write a message to the destination
   * @param message
   */
  public void write(String edge, Object message) {
    collection.collect(0, new TaskMessage(message, edge, taskId));
  }
}
