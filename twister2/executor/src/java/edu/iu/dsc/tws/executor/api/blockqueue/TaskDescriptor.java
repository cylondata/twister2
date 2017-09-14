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
package edu.iu.dsc.tws.executor.api.blockqueue;

import java.io.Serializable;
import java.util.Comparator;

/**
 * Created by vibhatha on 9/5/17.
 */
public class TaskDescriptor implements Comparable<TaskDescriptor>, Comparator<TaskDescriptor>, Serializable{

  private String id;
  private String taskName;

  public TaskDescriptor(String id, String taskName) {
    this.id = id;
    this.taskName = taskName;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public String getTaskName() {
    return taskName;
  }

  public void setTaskName(String taskName) {
    this.taskName = taskName;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof TaskDescriptor)) return false;

    TaskDescriptor that = (TaskDescriptor) o;

    if (getId() != null ? !getId().equals(that.getId()) : that.getId() != null) return false;
    return getTaskName() != null ? getTaskName().equals(that.getTaskName()) : that.getTaskName() == null;
  }

  @Override
  public int hashCode() {
    int result = getId() != null ? getId().hashCode() : 0;
    result = 31 * result + (getTaskName() != null ? getTaskName().hashCode() : 0);
    return result;
  }

  @Override
  public String toString() {
    return "TaskDescriptor{" +
        "id='" + id + '\'' +
        ", taskName='" + taskName + '\'' +
        '}';
  }

  @Override
  public int compareTo(TaskDescriptor o) {
    return 0;
  }

  @Override
  public int compare(TaskDescriptor o1, TaskDescriptor o2) {

    return 0;
  }
}
