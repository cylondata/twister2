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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import edu.iu.dsc.tws.api.comms.channel.TWSChannel;
import edu.iu.dsc.tws.api.compute.executor.ExecutionPlan;
import edu.iu.dsc.tws.api.compute.executor.ExecutorContext;
import edu.iu.dsc.tws.api.compute.executor.IExecution;
import edu.iu.dsc.tws.api.compute.executor.IExecutionHook;
import edu.iu.dsc.tws.api.compute.executor.IExecutor;
import edu.iu.dsc.tws.api.compute.executor.INodeInstance;
import edu.iu.dsc.tws.api.compute.executor.IParallelOperation;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.executor.core.ExecutionRuntime;

public class StreamingAllSharingExecutor implements IExecutor {
  private static final Logger LOG = Logger.getLogger(StreamingSharingExecutor.class.getName());

  /**
   * Number of threads to use
   */
  private int numThreads;

  /**
   * Number of threads
   */
  private ExecutorService threads;

  /**
   * Channel
   */
  private TWSChannel channel;

  /**
   * The configuration
   */
  private Config config;

  /**
   * worker id
   */
  private int workerId;

  /**
   * not stopped
   */
  private boolean notStopped = true;

  /*
   * clean up is called, so we cannot cleanup again
   */
  private boolean cleanUpCalled = false;

  /**
   * Wait for threads to finish
   */
  private CountDownLatch doneSignal;

  /**
   * Execution plan
   */
  private ExecutionPlan plan;

  /**
   * The execution hook
   */
  protected IExecutionHook executionHook;

  public StreamingAllSharingExecutor(Config cfg, int workerId, TWSChannel channel,
                                     ExecutionPlan executionPlan, IExecutionHook hook) {
    this.workerId = workerId;
    this.config = cfg;
    this.channel = channel;
    this.numThreads = ExecutorContext.threadsPerContainer(config);
    if (numThreads > 1) {
      this.threads = Executors.newFixedThreadPool(numThreads - 1,
          new ThreadFactoryBuilder().setNameFormat("executor-%d").setDaemon(true).build());
    }
    this.plan = executionPlan;
    this.executionHook = hook;
  }

  public boolean execute() {
    executionHook.beforeExecution();
    // lets create the runtime object
    ExecutionRuntime runtime = new ExecutionRuntime(ExecutorContext.jobName(config), plan, channel);
    // updated config
    this.config = Config.newBuilder().putAll(config).
        put(ExecutorContext.TWISTER2_RUNTIME_OBJECT, runtime).build();

    // go through the instances
    return runExecution();
  }

  @Override
  public boolean execute(boolean close) {
    return execute();
  }

  @Override
  public ExecutionPlan getExecutionPlan() {
    return plan;
  }

  public IExecution iExecute() {
    executionHook.beforeExecution();
    // lets create the runtime object
    ExecutionRuntime runtime = new ExecutionRuntime(ExecutorContext.jobName(config), plan, channel);
    // updated config
    this.config = Config.newBuilder().putAll(config).
        put(ExecutorContext.TWISTER2_RUNTIME_OBJECT, runtime).build();

    // go through the instances
    return runIExecution();
  }

  @Override
  public void close() {
    if (threads != null) {
      threads.shutdown();
    }
  }

  /**
   * Execution Method for Batch Tasks
   */
  public boolean runExecution() {
    Map<Integer, INodeInstance> nodes = plan.getNodes();

    if (nodes.size() == 0) {
      LOG.warning(String.format("Worker %d has zero assigned tasks, you may "
          + "have more workers than tasks", workerId));
      return true;
    }

    StreamWorker[] workers = scheduleExecution(nodes);
    StreamWorker worker = workers[0];
    // we progress until all the channel finish
    while (notStopped) {
      channel.progress();
      // the main thread call the run method of the 0th worker
      worker.runExecution();
    }

    cleanUp(nodes);
    return true;
  }

  private StreamWorker[] scheduleExecution(Map<Integer, INodeInstance> nodes) {
    // initialize finished
    List<INodeInstance> tasks = new ArrayList<>(nodes.values());

    // prepare the tasks
    for (INodeInstance node : tasks) {
      node.prepare(config);
    }

    StreamWorker[] workers = new StreamWorker[numThreads];

    final AtomicBoolean[] taskStatus = new AtomicBoolean[tasks.size()];
    final AtomicBoolean[] idleTasks = new AtomicBoolean[tasks.size()];
    for (int i = 0; i < tasks.size(); i++) {
      taskStatus[i] = new AtomicBoolean(false);
      idleTasks[i] = new AtomicBoolean(false);
    }
    doneSignal = new CountDownLatch(numThreads - 1);
    AtomicInteger idleCounter = new AtomicInteger(tasks.size());
    workers[0] = new StreamWorker(tasks, taskStatus, idleTasks, idleCounter);
    for (int i = 1; i < numThreads; i++) {
      StreamWorker task = new StreamWorker(tasks, taskStatus, idleTasks, idleCounter);
      threads.submit(task);
      workers[i] = task;
    }
    return workers;
  }

  private void cleanUp(Map<Integer, INodeInstance> nodes) {
    // lets wait for thread to finish
    try {
      doneSignal.await();
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted", e);
    }

    // clear the finished instances
    cleanUpCalled = true;
    // run the execution hook
    executionHook.afterExecution();
  }

  public IExecution runIExecution() {
    Map<Integer, INodeInstance> nodes = plan.getNodes();

    if (nodes.size() == 0) {
      LOG.warning(String.format("Worker %d has zero assigned tasks, you may "
          + "have more workers than tasks", workerId));
      return new NullExecutor();
    }

    StreamWorker[] workers = scheduleExecution(nodes);
    return new StreamExecution(plan, nodes, workers[0]);
  }

  @Override
  public boolean closeExecution() {
    Map<Integer, INodeInstance> nodes = plan.getNodes();

    if (nodes.size() == 0) {
      LOG.warning(String.format("Worker %d has zero assigned tasks, you may "
          + "have more workers than tasks", workerId));
      return true;
    }

    CommunicationWorker[] workers = scheduleWaitFor(nodes);
    CommunicationWorker worker = workers[0];
    // we progress until all the channel finish
    while (notStopped) {
      channel.progress();
      worker.runChannelComplete();
    }

    // at this point we are going to reset and close the operations
    close(plan, nodes);

    return true;
  }

  private CommunicationWorker[] scheduleWaitFor(Map<Integer, INodeInstance> nodes) {
    BlockingQueue<INodeInstance> tasks;

    tasks = new ArrayBlockingQueue<>(nodes.size() * 2);
    tasks.addAll(nodes.values());

    CommunicationWorker[] workers = new CommunicationWorker[numThreads];
    workers[0] = new CommunicationWorker(tasks);

    doneSignal = new CountDownLatch(numThreads - 1);
    for (int i = 1; i < numThreads; i++) {
      workers[i] = new CommunicationWorker(tasks);
      threads.submit(workers[i]);
    }
    return workers;
  }

  private void close(ExecutionPlan executionPlan, Map<Integer, INodeInstance> nodes) {
    // lets wait for thread to finish
    try {
      doneSignal.await();
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted", e);
    }

    List<IParallelOperation> ops = executionPlan.getParallelOperations();
    resetNodes(nodes, ops);

    // clean up the instances
    for (INodeInstance node : nodes.values()) {
      node.close();
    }

    // lets close the operations
    for (IParallelOperation op : ops) {
      op.close();
    }

    // clear the finished instances
    cleanUpCalled = true;
  }

  private void resetNodes(Map<Integer, INodeInstance> nodes, List<IParallelOperation> ops) {
    // clean up the instances
    for (INodeInstance node : nodes.values()) {
      node.reset();
    }

    // lets close the operations
    for (IParallelOperation op : ops) {
      op.reset();
    }
  }

  protected class CommunicationWorker implements Runnable {
    private BlockingQueue<INodeInstance> tasks;

    public CommunicationWorker(BlockingQueue<INodeInstance> tasks) {
      this.tasks = tasks;
    }

    @Override
    public void run() {
      while (notStopped) {
        runChannelComplete();
      }
      doneSignal.countDown();
    }

    private void runChannelComplete() {
      try {
        INodeInstance nodeInstance = tasks.poll();
        if (nodeInstance != null) {
          boolean complete = nodeInstance.isComplete();
          if (!complete) {
            // we need to further execute this task
            tasks.offer(nodeInstance);
          }
        }
      } catch (Throwable t) {
        LOG.log(Level.SEVERE, String.format("%d Error in executor", workerId), t);
        throw new RuntimeException("Error occurred in execution of task", t);
      }
    }
  }

  protected class StreamWorker implements Runnable {
    // round robin mode
    private List<INodeInstance> tasks;
    private AtomicBoolean[] ignoreIndex;
    private AtomicBoolean[] idleTasks;
    private int lastIndex;
    private AtomicInteger activeCounter;

    public StreamWorker(List<INodeInstance> tasks,
                        AtomicBoolean[] ignoreIndex, AtomicBoolean[] idle,
                        AtomicInteger activeCounter) {
      this.tasks = tasks;
      this.ignoreIndex = ignoreIndex;
      this.idleTasks = idle;
      this.activeCounter = activeCounter;
    }

    private int getNext() {
      if (this.lastIndex == tasks.size()) {
        this.lastIndex = 0;
      }

      if (ignoreIndex[this.lastIndex].compareAndSet(false, true)) {
        return this.lastIndex++;
      }
      this.lastIndex++;
      return -1;
    }

    @Override
    public void run() {
      while (notStopped) {
        runExecution();
      }
      doneSignal.countDown();
    }

    private void runExecution() {
      try {
        int nodeInstanceIndex = this.getNext();
        if (nodeInstanceIndex != -1) {
          INodeInstance nodeInstance = this.tasks.get(nodeInstanceIndex);
          boolean needsFurther = nodeInstance.execute();
          // need further execution
          if (needsFurther) {
            // if we were idle, we are no longer idle
            if (idleTasks[nodeInstanceIndex].compareAndSet(true, false)) {
              activeCounter.getAndIncrement();
            }
          } else {
            // if we don't need further execution at this time and we were not idle before
            if (this.idleTasks[nodeInstanceIndex].compareAndSet(false, true)) {
              int count = activeCounter.decrementAndGet();
              // if we reach 0, we need to sleep
              if (count == 0) {
                LockSupport.parkNanos(1);
              }
            } else {
              // if we were idle before, check if the count is still 0
              int count = activeCounter.get();
              // if we reach 0, we need to sleep
              if (count == 0) {
                LockSupport.parkNanos(1);
              }
            }
          }
          this.ignoreIndex[nodeInstanceIndex].set(false);
        }
      } catch (Throwable t) {
        LOG.log(Level.SEVERE, String.format("%d Error in executor", workerId), t);
        throw new RuntimeException("Error occurred in execution of task", t);
      }
    }
  }

  private class StreamExecution implements IExecution {
    /**
     * Keep the node map
     */
    private Map<Integer, INodeInstance> nodeMap;

    private ExecutionPlan executionPlan;

    private boolean taskExecution = true;

    private StreamWorker mainWorker;

    private CommunicationWorker worker;

    StreamExecution(ExecutionPlan executionPlan, Map<Integer, INodeInstance> nodeMap,
                    StreamWorker mainWorker) {
      this.nodeMap = nodeMap;
      this.executionPlan = executionPlan;
      this.mainWorker = mainWorker;
    }

    @Override
    public boolean waitForCompletion() {
      // we progress until all the channel finish
      while (notStopped) {
        channel.progress();
        mainWorker.runExecution();
      }

      cleanUp(nodeMap);

      // now wait for it
      closeExecution();
      return true;
    }

    @Override
    public boolean progress() {
      if (taskExecution) {
        // we progress until all the channel finish
        if (notStopped) {
          channel.progress();
          mainWorker.runExecution();
          return true;
        }
        // clean up
        cleanUp(nodeMap);
        cleanUpCalled = false;
        // if we finish, lets schedule
        CommunicationWorker[] workers = scheduleWaitFor(nodeMap);
        this.worker = workers[0];
        taskExecution = false;
      }

      // we progress until all the channel finish
      channel.progress();
      worker.runChannelComplete();
      return false;
    }

    public void close() {
      if (notStopped) {
        throw new RuntimeException("We need to stop the execution before close");
      }

      if (!cleanUpCalled) {
        StreamingAllSharingExecutor.this.close(executionPlan, nodeMap);
        cleanUpCalled = true;
      } else {
        throw new RuntimeException("Close is called on a already closed execution");
      }
    }

    @Override
    public void stop() {
      notStopped = false;
    }
  }
}
