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
package edu.iu.dsc.tws.task.api.schedule;

/**
 * This static class is responsible for assigning the task name, task id, task index, and resource
 * requirements of the task instances.
 */
public class TaskInstancePlan implements Comparable<TaskInstancePlan> {
  private final String taskName;
  private final int taskId;
  private final int taskIndex;
  private final Resource resource;

  public TaskInstancePlan(String taskName, int taskId, int taskIndex, Resource resource) {
    this.taskName = taskName;
    this.taskId = taskId;
    this.taskIndex = taskIndex;
    this.resource = resource;
  }

  public TaskInstancePlan(TaskInstanceId taskInstanceId, Resource resource) {
    this.taskName = taskInstanceId.getTaskName();
    this.taskId = taskInstanceId.getTaskId();
    this.taskIndex = taskInstanceId.getTaskIndex();
    this.resource = resource;
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

  public Resource getResource() {
    return resource;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    TaskInstancePlan that = (TaskInstancePlan) o;

    return getTaskName().equals(that.getTaskName())
        && getTaskId() == that.getTaskId()
        && getTaskIndex() == that.getTaskIndex()
        && getResource().equals(that.getResource());
  }

  @Override
  public int hashCode() {
    int result = getTaskName().hashCode();
    result = 31 * result + ((Integer) getTaskId()).hashCode();
    result = 31 * result + ((Integer) getTaskIndex()).hashCode();
    result = 31 * result + getResource().hashCode();
    return result;
  }

  @Override
  public int compareTo(TaskInstancePlan o) {
    if (this.taskId == o.getTaskId()) {
      return Integer.compare(this.taskIndex, o.taskIndex);
    }
    return Integer.compare(this.taskId, o.taskId);
  }
}
