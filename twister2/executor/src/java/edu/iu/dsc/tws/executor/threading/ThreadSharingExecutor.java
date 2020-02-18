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

import edu.iu.dsc.tws.api.comms.channel.TWSChannel;
import edu.iu.dsc.tws.api.compute.executor.ExecutionPlan;
import edu.iu.dsc.tws.api.compute.executor.ExecutorContext;
import edu.iu.dsc.tws.api.compute.executor.IExecution;
import edu.iu.dsc.tws.api.compute.executor.IExecutionHook;
import edu.iu.dsc.tws.api.compute.executor.IExecutor;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.executor.core.ExecutionRuntime;

/**
 * Abstract class for thread sharing executors
 */
public abstract class ThreadSharingExecutor implements IExecutor {
  private static final Logger LOG = Logger.getLogger(ThreadSharingExecutor.class.getName());

  /**
   * Number of threads
   */
  protected int numThreads;

  /**
   * Thread service
   */
  protected ExecutorService threads;

  /**
   * Channel
   */
  protected TWSChannel channel;

  /**
   * The configuration
   */
  protected Config config;

  /**
   * Execution plan
   */
  protected ExecutionPlan executionPlan;

  /**
   * The execution hook
   */
  protected IExecutionHook executionHook;

  public ThreadSharingExecutor(Config config, TWSChannel ch, ExecutionPlan plan,
                               IExecutionHook hook) {
    this.config = config;
    this.channel = ch;
    this.numThreads = ExecutorContext.threadsPerContainer(config);
    Thread.UncaughtExceptionHandler hndler = new Thread.UncaughtExceptionHandler() {
      public void uncaughtException(Thread th, Throwable ex) {
        throw new RuntimeException(ex);
      }
    };
    this.threads = Executors.newFixedThreadPool(numThreads,
        new ThreadFactoryBuilder()
            .setNameFormat("executor-%d")
            .setDaemon(true)
            .setUncaughtExceptionHandler(hndler)
            .build());
    this.executionPlan = plan;
    this.executionHook = hook;
  }

  public boolean execute() {
    // run the execution hook
    executionHook.beforeExecution();
    // lets create the runtime object
    ExecutionRuntime runtime = new ExecutionRuntime(ExecutorContext.jobName(config),
        executionPlan, channel);
    // updated config
    this.config = Config.newBuilder().putAll(config).
        put(ExecutorContext.TWISTER2_RUNTIME_OBJECT, runtime).build();

    // go through the instances
    return runExecution();
  }

  public IExecution iExecute() {
    executionHook.beforeExecution();
    // lets create the runtime object
    ExecutionRuntime runtime = new ExecutionRuntime(
        ExecutorContext.jobName(config), executionPlan, channel);
    // updated config
    this.config = Config.newBuilder().putAll(config).
        put(ExecutorContext.TWISTER2_RUNTIME_OBJECT, runtime).build();

    // go through the instances
    return runIExecution();
  }

  /**
   * Specific implementation needs to implement this method
   *
   * @return weather we executed successfully
   */
  public abstract boolean runExecution();

  /**
   * Specific implementation needs to implement this method
   *
   * @return weather we executed successfully
   */
  public abstract IExecution runIExecution();

  @Override
  public void close() {
    threads.shutdown();
    long start = System.currentTimeMillis();
    // wait for 5 seconds until channel completes
    while (!channel.isComplete()) {
      if (System.currentTimeMillis() - start > 1000) {
        break;
      }
    }
  }

  @Override
  public ExecutionPlan getExecutionPlan() {
    return executionPlan;
  }
}
