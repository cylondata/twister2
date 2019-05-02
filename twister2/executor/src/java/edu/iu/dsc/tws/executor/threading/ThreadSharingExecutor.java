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
package edu.iu.dsc.tws.executor.threading;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.TWSChannel;
import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.executor.api.IExecution;
import edu.iu.dsc.tws.executor.api.IExecutor;
import edu.iu.dsc.tws.executor.core.ExecutionRuntime;
import edu.iu.dsc.tws.executor.core.ExecutorContext;

/**
 * Abstract class for thread sharing executors
 */
public abstract class ThreadSharingExecutor implements  IExecutor {
  private static final Logger LOG = Logger.getLogger(ThreadSharingExecutor.class.getName());

  protected int numThreads;

  protected ExecutorService threads;

  protected TWSChannel channel;

  protected Config config;

  public ThreadSharingExecutor(Config config, TWSChannel ch) {
    this.config = config;
    this.channel = ch;
    this.numThreads = ExecutorContext.threadsPerContainer(config);
    this.threads = Executors.newFixedThreadPool(numThreads,
        new ThreadFactoryBuilder().setNameFormat("executor-%d").setDaemon(true).build());
  }

  public boolean execute(ExecutionPlan plan) {
    // lets create the runtime object
    ExecutionRuntime runtime = new ExecutionRuntime(ExecutorContext.jobName(config), plan, channel);
    // updated config
    this.config = Config.newBuilder().putAll(config).
        put(ExecutorContext.TWISTER2_RUNTIME_OBJECT, runtime).build();

    // go through the instances
    return runExecution(plan);
  }

  public IExecution iExecute(ExecutionPlan plan) {
    // lets create the runtime object
    ExecutionRuntime runtime = new ExecutionRuntime(ExecutorContext.jobName(config), plan, channel);
    // updated config
    this.config = Config.newBuilder().putAll(config).
        put(ExecutorContext.TWISTER2_RUNTIME_OBJECT, runtime).build();

    // go through the instances
    return runIExecution(plan);
  }

  /**
   * Specific implementation needs to implement this method
   * @return weather we executed successfully
   */
  public abstract boolean runExecution(ExecutionPlan plan);

  /**
   * Specific implementation needs to implement this method
   * @return weather we executed successfully
   */
  public abstract IExecution runIExecution(ExecutionPlan plan);

  @Override
  public void close() {
    threads.shutdown();
    long start = System.currentTimeMillis();
    // wait for 5 seconds until channel completes
    while (!channel.isComplete()) {
      if (System.currentTimeMillis() - start > 5000) {
        break;
      }
    }
  }
}
