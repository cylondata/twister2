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
import java.util.concurrent.TimeUnit;

import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.core.TWSCommunication;
import edu.iu.dsc.tws.task.api.RunnableTask;
import edu.iu.dsc.tws.task.api.Task;

/**
 * Class that will handle the task execution. Each task executor will manage an task execution
 * thread pool that will be used to execute the tasks that are assigned to the particular task
 * executor.
 */
public class TaskExecutorCachedThreadPool {

  private static ThreadPoolExecutor executorPool;
  private TWSCommunication channel;
  private DataFlowOperation direct;
  private boolean progres = false;



  public TaskExecutorCachedThreadPool() {
    initThreadPool(ExecutorContext.EXECUTOR_CORE_POOL_SIZE,ExecutorContext.EXECUTOR_MAX_POOL_SIZE,ExecutorContext.EXECUTOR_POOL_KEEP_ALIVE_TIME);
  }

  public TaskExecutorCachedThreadPool(int poolSize) {
    initThreadPool(poolSize,ExecutorContext.EXECUTOR_MAX_POOL_SIZE,ExecutorContext.EXECUTOR_POOL_KEEP_ALIVE_TIME);
  }

  public TaskExecutorCachedThreadPool(int poolSize, int maxPoolSize, long keepAliveTime) {
    initThreadPool(poolSize,maxPoolSize,keepAliveTime);
  }

  /**
   * Init task executor
   * @param twscom
   * @param dfo
   */
  public void init(TWSCommunication twscom, DataFlowOperation dfo) {
    this.channel = twscom;
    this.direct = dfo;
    this.progres = true;
  }
  /**
   * Submit the task to run in the thread pool.
   * @param task task to be run
   * @return returns true if the task was submitted and queued
   */
  public boolean submit(Task task) {

    executorPool.submit(new RunnableTask(task));
    return true;
  }

  public void progres() {
    while (progres) { //This can be done in a separate thread if that is more suitable
      channel.progress();
      direct.progress();
      Thread.yield();
    }
  }

  public void setProgress(boolean value) {
    this.progres = value;
  }

  private void initThreadPool(int corePoolSize, int maxPoolSize, long keepAliveTime){
    executorPool = (ThreadPoolExecutor) Executors.newCachedThreadPool();
    executorPool.setCorePoolSize(corePoolSize);
    executorPool.setMaximumPoolSize(maxPoolSize);
    executorPool.setKeepAliveTime(keepAliveTime, TimeUnit.MILLISECONDS);

  }
}
