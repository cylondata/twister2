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

public class TaskInstanceId {
  private final String taskName;
  private final int taskId;
  private final int taskIndex;

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

    return taskId == that.taskId && taskIndex == that.taskIndex
        && (taskName != null ? taskName.equals(that.taskName) : that.taskName == null);
  }

  @Override
  public int hashCode() {
    int result = taskName.hashCode();
    result = 31 * result + taskId;
    result = 31 * result + taskIndex;
    return result;
  }
}

