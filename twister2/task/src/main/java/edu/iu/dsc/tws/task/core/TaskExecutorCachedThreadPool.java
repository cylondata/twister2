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

import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.TWSChannel;
import edu.iu.dsc.tws.task.api.INode;
import edu.iu.dsc.tws.task.api.TaskExecutor;

/**
 * Class that will handle the task execution. Each task executor will manage an task execution
 * thread pool that will be used to execute the tasks that are assigned to the particular task
 * executor.
 */
public class TaskExecutorCachedThreadPool implements TaskExecutor {

  private static ThreadPoolExecutor executorPool;
  private TWSChannel channel;
  private DataFlowOperation direct;
  private boolean progres = false;


  public TaskExecutorCachedThreadPool() {
    initExecutionPool(ExecutorContext.EXECUTOR_CORE_POOL_SIZE);
  }

  public TaskExecutorCachedThreadPool(int poolSize) {
    initExecutionPool(poolSize);
  }

  public TaskExecutorCachedThreadPool(int poolSize, int maxPoolSize, long keepAliveTime) {
    initExecutionPool(poolSize, maxPoolSize, keepAliveTime);
  }

  /**
   * Init task executor
   */
  public void init(TWSChannel twscom, DataFlowOperation dfo) {
    this.channel = twscom;
    this.direct = dfo;
    this.progres = true;
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

  private void initExecutionPool(int corePoolSize, int maxPoolSize, long keepAliveTime) {
    executorPool = (ThreadPoolExecutor) Executors.newCachedThreadPool();
    executorPool.setCorePoolSize(corePoolSize);
    executorPool.setMaximumPoolSize(maxPoolSize);
    executorPool.setKeepAliveTime(keepAliveTime, TimeUnit.MILLISECONDS);

  }

  @Override
  public void initExecutionPool(int coreSize) {
    initExecutionPool(coreSize, ExecutorContext.EXECUTOR_MAX_POOL_SIZE,
        ExecutorContext.EXECUTOR_POOL_KEEP_ALIVE_TIME);
  }

  /**
   * Submit the task to run in the thread pool.
   *
   * @param task task to be run
   * @return returns true if the task was submitted and queued
   */
  @Override
  public boolean submitTask(INode task) {
    executorPool.submit(new RunnableTask(task));
    return true;
  }

  @Override
  public boolean submitTask(int tid) {
    return false;
  }

  @Override
  public Set<Integer> getRunningTasks() {
    return null;
  }

  @Override
  public boolean addRunningTasks(int tid) {
    return false;
  }

  @Override
  public boolean removeRunningTask(int tid) {
    return false;
  }

  @Override
  public boolean registerTask(INode task) {
    return false;
  }

  @Override
  public <T> boolean submitMessage(int qid, T message) {
    return false;
  }
}
