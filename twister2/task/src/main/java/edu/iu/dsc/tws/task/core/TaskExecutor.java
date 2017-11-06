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
package edu.iu.dsc.tws.task.core;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import edu.iu.dsc.tws.task.api.Task;
import edu.iu.dsc.tws.task.core.ExecutorContext;

/**
 * Class that will handle the task execution. Each task executor will manage an task execution
 * thread pool that will be used to execute the tasks that are assigned to the particular task
 * executor
 */
public class TaskExecutor {

  private static ThreadPoolExecutor executorPool;


  public TaskExecutor(){
    executorPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(ExecutorContext.EXECUTOR_CORE_POOL_SIZE);
  }

  public TaskExecutor(int poolSize){
    executorPool = (ThreadPoolExecutor) Executors.newFixedThreadPool(poolSize);
  }

  /**
   * Submit the task to run in the thread pool.
   * @param task task to be run
   * @return returns true if the task was submitted and queued
   */
  public boolean submit(Task task){
    executorPool.submit(task);
    return true;
  }

}
