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

package edu.iu.dsc.tws.api.compute.schedule.elements;

import java.util.Objects;

/**
 * This class has getter and setter property to get and set the values of task name, task id, and
 * task indexes. <code>TaskInstanceId</code> is equal to another task id, if name, id and index are
 * equal.
 */
public class TaskInstanceId {
  /**
   * Task name
   */
  private final String taskName;

  /**
   * Task id
   */
  private final int taskId;

  /**
   * Task index
   */
  private final int taskIndex;

  /**
   * Create a task instance id
   *
   * @param taskName name
   * @param taskId task id
   * @param taskIndex task index
   */
  public TaskInstanceId(String taskName, int taskId, int taskIndex) {
    this.taskName = taskName;
    this.taskId = taskId;
    this.taskIndex = taskIndex;
  }

  public String getTaskName() {
    return taskName;
  }

  public int getTaskId() {
    return taskId;
  }

  public int getTaskIndex() {
    return taskIndex;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof TaskInstanceId)) {
      return false;
    }

    TaskInstanceId that = (TaskInstanceId) o;

    if (taskId != that.taskId) {
      return false;
    }
    if (taskIndex != that.taskIndex) {
      return false;
    }
    return Objects.equals(taskName, that.taskName);
  }

  @Override
  public int hashCode() {
    int result = taskName != null ? taskName.hashCode() : 0;
    result = 31 * result + taskId;
    result = 31 * result + taskIndex;
    return result;
  }

}
