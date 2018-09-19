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
package edu.iu.dsc.tws.examples.verification;

import java.util.List;

import edu.iu.dsc.tws.task.graph.OperationMode;

public class ExperimentData {

  private Object input;
  private Object output;
  private List<Integer> taskStages;
  private int workerId;
  private int numOfWorkers;
  private int taskId;
  private int iterations;
  private OperationMode operationMode;

  public ExperimentData() {

  }

  public Object getInput() {
    return input;
  }

  public void setInput(Object input) {
    this.input = input;
  }

  public Object getOutput() {
    return output;
  }

  public void setOutput(Object output) {
    this.output = output;
  }

  public List<Integer> getTaskStages() {
    return taskStages;
  }

  public void setTaskStages(List<Integer> taskList) {
    this.taskStages = taskList;
  }

  public int getWorkerId() {
    return workerId;
  }

  public void setWorkerId(int workerId) {
    this.workerId = workerId;
  }

  public OperationMode getOperationMode() {
    return operationMode;
  }

  public void setOperationMode(OperationMode operationMode) {
    this.operationMode = operationMode;
  }

  public int getIterations() {
    return iterations;
  }

  public void setIterations(int iterations) {
    this.iterations = iterations;
  }

  public void setTaskId(int taskId) {
    this.taskId = taskId;
  }

  public int getTaskId() {
    return taskId;
  }

  public int getNumOfWorkers() {
    return numOfWorkers;
  }

  public void setNumOfWorkers(int numOfWorkers) {
    this.numOfWorkers = numOfWorkers;
  }
}
