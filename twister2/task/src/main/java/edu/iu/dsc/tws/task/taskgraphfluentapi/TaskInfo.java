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
package edu.iu.dsc.tws.task.taskgraphfluentapi;

import edu.iu.dsc.tws.task.taskgraphbuilder.DataflowOperation;

public class TaskInfo {

  public ITaskInfo sourceTask;
  public ITaskInfo[] targetTask;
  public DataflowOperation dataflowOperation;

  public ITaskInfo getSourceTask() {
    return sourceTask;
  }

  public void setSourceTask(ITaskInfo sourceTask) {
    this.sourceTask = sourceTask;
  }

  public ITaskInfo[] getTargetTask() {
    return targetTask;
  }

  public void setTargetTask(ITaskInfo[] targetTask) {
    this.targetTask = targetTask;
  }

  public DataflowOperation getDataflowOperation() {
    return dataflowOperation;
  }

  public void setDataflowOperation(DataflowOperation dataflowOperation) {
    this.dataflowOperation = dataflowOperation;
  }
}

