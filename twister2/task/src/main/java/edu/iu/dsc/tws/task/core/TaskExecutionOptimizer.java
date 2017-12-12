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
package edu.iu.dsc.tws.task.core;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import edu.iu.dsc.tws.task.api.TaskExecutor;

/**
 * Class that is responsible ot optimizing the task execution. The class is responsible of creating
 * task pipelines. etc. Each jov will have a separate optimizer. Ideally the scheduler will call
 * this class
 */
public class TaskExecutionOptimizer {


  /**
   * The task executor associated with this Optimizer
   */
  private TaskExecutor taskExecutor;

  public Map<Integer, ArrayList<Integer>> getQueuexTaskInput() {
    return queuexTaskInput;
  }

  /**
   * Reverse mapping which maps each queue id to its input task
   */
  private Map<Integer, ArrayList<Integer>> queuexTaskInput =
      new HashMap<Integer, ArrayList<Integer>>();

  /**
   * Reverse mapping which maps each queue id to its output task
   */
  private Map<Integer, ArrayList<Integer>> queuexTaskOutput =
      new HashMap<Integer, ArrayList<Integer>>();

  /**
   * Hashmap that keeps the tasks and its input queues
   */
  private Map<Integer, ArrayList<Integer>> taskInputQueues =
      new HashMap<Integer, ArrayList<Integer>>();

  /**
   * Hashmap that keeps the tasks and its output queues
   */
  private Map<Integer, ArrayList<Integer>> taskOutputQueues =
      new HashMap<Integer, ArrayList<Integer>>();


  public TaskExecutionOptimizer(TaskExecutor taskExecutor) {
    this.taskExecutor = taskExecutor;
  }

  public TaskExecutor getTaskExecutor() {
    return taskExecutor;
  }

  public void setTaskExecutor(TaskExecutor taskExecutor) {
    this.taskExecutor = taskExecutor;
  }

  /**
   * Needs to be called once all the tasks that need to be submitted to the optimizer are submitted
   * the optimizer will run and submit tasks to the TaskExecutor only once this method has been
   * called
   */
  public void finish() {
    runOptimizer();
  }

  /**
   * Pefroms analysis on the task information to optimize the execution. once done it will
   * submit the tasks to the TaskExecutor
   */
  private void runOptimizer() {

  }

  public void setQueuexTaskInput(Map<Integer, ArrayList<Integer>> queuexTaskInput) {
    this.queuexTaskInput = queuexTaskInput;
  }

  public Map<Integer, ArrayList<Integer>> getQueuexTaskOutput() {
    return queuexTaskOutput;
  }

  public void setQueuexTaskOutput(Map<Integer, ArrayList<Integer>> queuexTaskOutput) {
    this.queuexTaskOutput = queuexTaskOutput;
  }

  public Map<Integer, ArrayList<Integer>> getTaskInputQueues() {
    return taskInputQueues;
  }

  public void setTaskInputQueues(Map<Integer, ArrayList<Integer>> taskInputQueues) {
    this.taskInputQueues = taskInputQueues;
  }

  public Map<Integer, ArrayList<Integer>> getTaskOutputQueues() {
    return taskOutputQueues;
  }

  public void setTaskOutputQueues(Map<Integer, ArrayList<Integer>> taskOutputQueues) {
    this.taskOutputQueues = taskOutputQueues;
  }
}
