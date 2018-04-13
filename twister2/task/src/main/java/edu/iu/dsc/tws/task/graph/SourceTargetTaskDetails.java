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
package edu.iu.dsc.tws.task.graph;

import edu.iu.dsc.tws.task.api.ITask;

public class SourceTargetTaskDetails {

  private ITask sourceTask;
  private ITask targetTask;
  private String dataflowOperationName;
  private Edge dataflowOperation;

  public ITask getSourceTask() {
    return sourceTask;
  }

  public void setSourceTask(ITask sourceTask) {
    this.sourceTask = sourceTask;
  }

  public ITask getTargetTask() {
    return targetTask;
  }

  public void setTargetTask(ITask targetTask) {
    this.targetTask = targetTask;
  }

  public String getDataflowOperationName() {
    return dataflowOperationName;
  }

  public void setDataflowOperationName(String dataflowOperationName1) {
    this.dataflowOperationName = dataflowOperationName1;
  }

  public Edge getDataflowOperation() {
    return dataflowOperation;
  }

  public void setDataflowOperation(Edge dataflowOperation) {
    this.dataflowOperation = dataflowOperation;
  }
}
