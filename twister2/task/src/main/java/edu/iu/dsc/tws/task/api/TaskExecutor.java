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

import java.util.Set;

/**
 * Base interface for all Task executors. The task executor is responsible of executing the tasks
 * that are submitted to it from other components
 */
public interface TaskExecutor {

  /**
   * Initialize the Execution pool. The execution pool is the set of processes or threads that are
   * used to execute tasks.
   *
   * @param coreSize the size of the execution pool
   */
  void initExecutionPool(int coreSize);

  /**
   * Submit task to the executor
   *
   * @param task the task to be submitted
   */
  boolean submitTask(INode task);

  /**
   * Submit task with tid. This method assumes the actual task is already submitted through another
   * method
   *
   * @param tid the task id of the task to submit.
   */
  boolean submitTask(int tid);

  /**
   * Returns the set of tasks currently running in this executor
   */
  Set<Integer> getRunningTasks();

  /**
   * Adds tasks to the running task set. Needs to be called after the task starts running in the
   * executor
   */
  boolean addRunningTasks(int tid);

  /**
   * Remove task from the running task set. Needs to be called after the task has completed
   * execution
   */
  boolean removeRunningTask(int tid);

  /**
   * Register the task in the executor
   */
  boolean registerTask(INode task);

  /**
   * Method submits messages to a given queue
   *
   * @param qid id of the queue
   * @param message message to submit
   * @param <T> type of message
   */
  <T> boolean submitMessage(int qid, T message);

}
