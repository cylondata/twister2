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
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

import edu.iu.dsc.tws.api.comms.channel.TWSChannel;
import edu.iu.dsc.tws.api.compute.executor.ExecutionPlan;
import edu.iu.dsc.tws.api.compute.executor.ExecutionState;
import edu.iu.dsc.tws.api.compute.executor.ExecutorContext;
import edu.iu.dsc.tws.api.compute.executor.IExecution;
import edu.iu.dsc.tws.api.compute.executor.IExecutor;
import edu.iu.dsc.tws.api.compute.executor.INodeInstance;
import edu.iu.dsc.tws.api.compute.executor.IParallelOperation;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.executor.core.ExecutionRuntime;

public class BatchSharingExecutor2 implements IExecutor {
  private static final Logger LOG = Logger.getLogger(BatchSharingExecutor2.class.getName());

  protected int numThreads;

  protected ExecutorService threads;

  protected TWSChannel channel;

  protected Config config;

  // keep track of finished executions
  private AtomicInteger finishedInstances = new AtomicInteger(0);

  // worker id
  private int workerId;

  // not stopped
  private boolean notStopped = true;

  // clean up is called
  private boolean cleanUpCalled = false;

  /**
   * Wait for threads to finsih
   */
  private CountDownLatch doneSignal;

  public BatchSharingExecutor2(Config cfg, int workerId, TWSChannel channel) {
    this.workerId = workerId;
    this.config = cfg;
    this.channel = channel;
    this.numThreads = ExecutorContext.threadsPerContainer(config);
    if (numThreads > 1) {
      this.threads = Executors.newFixedThreadPool(numThreads - 1,
          new ThreadFactoryBuilder().setNameFormat("executor-%d").setDaemon(true).build());
    }
  }

  public boolean execute(ExecutionPlan plan) {
    // lets create the runtime object
    ExecutionRuntime runtime = new ExecutionRuntime(ExecutorContext.jobName(config), plan, channel);
    // updated config
    this.config = Config.newBuilder().putAll(config).
        put(ExecutorContext.TWISTER2_RUNTIME_OBJECT, runtime).build();

    // if this is a previously executed plan we have to reset the nodes
    if (plan.getExecutionState() == ExecutionState.EXECUTED) {
      resetNodes(plan.getNodes(), plan.getParallelOperations());
    }

    // go through the instances
    return runExecution(plan);
  }

  public IExecution iExecute(ExecutionPlan plan) {
    // lets create the runtime object
    ExecutionRuntime runtime = new ExecutionRuntime(ExecutorContext.jobName(config), plan, channel);
    // updated config
    this.config = Config.newBuilder().putAll(config).
        put(ExecutorContext.TWISTER2_RUNTIME_OBJECT, runtime).build();

    // if this is a previously executed plan we have to reset the nodes
    if (plan.getExecutionState() == ExecutionState.EXECUTED) {
      resetNodes(plan.getNodes(), plan.getParallelOperations());
    }

    // go through the instances
    return runIExecution(plan);
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
  public boolean runExecution(ExecutionPlan executionPlan) {
    Map<Integer, INodeInstance> nodes = executionPlan.getNodes();

    if (nodes.size() == 0) {
      LOG.warning(String.format("Worker %d has zero assigned tasks, you may "
          + "have more workers than tasks", workerId));
      return true;
    }

    BatchWorker[] workers = scheduleExecution(nodes);
    BatchWorker worker = workers[0];
    // we progress until all the channel finish
    while (notStopped && finishedInstances.get() != nodes.size()) {
      channel.progress();
      // the main thread call the run method of the 0th worker
      worker.runExecution();
    }

    cleanUp(executionPlan, nodes);
    return true;
  }

  private BatchWorker[] scheduleExecution(Map<Integer, INodeInstance> nodes) {
    // initialize finished
    List<INodeInstance> tasks = new ArrayList<>(nodes.values());

    // prepare the tasks
    for (INodeInstance node : tasks) {
      node.prepare(config);
    }

    BatchWorker[] batchWorkers = new BatchWorker[numThreads];

    final AtomicBoolean[] taskStatus = new AtomicBoolean[tasks.size()];
    for (int i = 0; i < tasks.size(); i++) {
      taskStatus[i] = new AtomicBoolean(false);
    }
    doneSignal = new CountDownLatch(numThreads - 1);
    batchWorkers[0] = new BatchWorker(tasks, taskStatus);
    for (int i = 1; i < numThreads; i++) {
      BatchWorker task = new BatchWorker(tasks, taskStatus);
      threads.submit(task);
      batchWorkers[i] = task;
    }
    return batchWorkers;
  }

  private void cleanUp(ExecutionPlan executionPlan, Map<Integer, INodeInstance> nodes) {
    // lets wait for thread to finish
    try {
      doneSignal.await();
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted", e);
    }

    // we set the execution state here
    executionPlan.setExecutionState(ExecutionState.EXECUTED);

    // clear the finished instances
    finishedInstances.set(0);
    cleanUpCalled = true;
  }

  public IExecution runIExecution(ExecutionPlan executionPlan) {
    Map<Integer, INodeInstance> nodes = executionPlan.getNodes();

    if (nodes.size() == 0) {
      LOG.warning(String.format("Worker %d has zero assigned tasks, you may "
          + "have more workers than tasks", workerId));
      return new NullExecutor();
    }

    BatchWorker[] workers = scheduleExecution(nodes);
    return new BatchExecution(executionPlan, nodes, workers[0]);
  }

  @Override
  public boolean waitFor(ExecutionPlan plan) {
    Map<Integer, INodeInstance> nodes = plan.getNodes();

    if (nodes.size() == 0) {
      LOG.warning(String.format("Worker %d has zero assigned tasks, you may "
          + "have more workers than tasks", workerId));
      return true;
    }

    CommunicationWorker[] workers = scheduleWaitFor(nodes);
    CommunicationWorker worker = workers[0];
    // we progress until all the channel finish
    while (notStopped && finishedInstances.get() != nodes.size()) {
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
    finishedInstances.set(0);
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
        if (!runChannelComplete()) {
          break;
        }
      }
      doneSignal.countDown();
    }

    private boolean runChannelComplete() {
      try {
        INodeInstance nodeInstance = tasks.poll();
        if (nodeInstance != null) {
          boolean complete = nodeInstance.isComplete();
          if (complete) {
            finishedInstances.incrementAndGet(); //(nodeInstance.getId(), true);
          } else {
            // we need to further execute this task
            tasks.offer(nodeInstance);
          }
          return true;
        } else {
          return false;
        }
      } catch (Throwable t) {
        LOG.log(Level.SEVERE, String.format("%d Error in executor", workerId), t);
        throw new RuntimeException("Error occurred in execution of task", t);
      }
    }
  }

  protected class BatchWorker implements Runnable {

    //round robin mode
    private List<INodeInstance> tasks;
    private AtomicBoolean[] ignoreIndex;
    private int lastIndex;

    public BatchWorker(List<INodeInstance> tasks, AtomicBoolean[] ignoreIndex) {
      this.tasks = tasks;
      this.ignoreIndex = ignoreIndex;
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
      while (notStopped && finishedInstances.get() != tasks.size()) {
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
          if (!needsFurther) {
            finishedInstances.incrementAndGet(); //(nodeInstance.getId(), true);
          } else {
            //need further execution
            this.ignoreIndex[nodeInstanceIndex].set(false);
          }
        }
      } catch (Throwable t) {
        LOG.log(Level.SEVERE, String.format("%d Error in executor", workerId), t);
        throw new RuntimeException("Error occurred in execution of task", t);
      }
    }
  }

  private class BatchExecution implements IExecution {
    private Map<Integer, INodeInstance> nodeMap;

    private ExecutionPlan executionPlan;

    private BlockingQueue<INodeInstance> tasks;

    private boolean taskExecution = true;

    private BatchWorker mainWorker;

    private CommunicationWorker worker;

    BatchExecution(ExecutionPlan executionPlan, Map<Integer, INodeInstance> nodeMap,
                   BatchWorker mainWorker) {
      this.nodeMap = nodeMap;
      this.executionPlan = executionPlan;
      this.mainWorker = mainWorker;

      tasks = new ArrayBlockingQueue<>(nodeMap.size() * 2);
      tasks.addAll(nodeMap.values());
    }

    @Override
    public boolean waitForCompletion() {
      // we progress until all the channel finish
      while (notStopped && finishedInstances.get() != nodeMap.size()) {
        channel.progress();
        mainWorker.runExecution();
      }
      // we are going to set to executed
      executionPlan.setExecutionState(ExecutionState.EXECUTED);

      cleanUp(executionPlan, nodeMap);

      // now wait for it
      waitFor(executionPlan);
      return true;
    }

    @Override
    public boolean progress() {
      if (taskExecution) {
        // we progress until all the channel finish
        if (finishedInstances.get() != nodeMap.size()) {
          channel.progress();
          mainWorker.runExecution();
          return true;
        }
        // lets set the execution state here
        executionPlan.setExecutionState(ExecutionState.EXECUTED);
        // clean up
        cleanUp(executionPlan, nodeMap);
        cleanUpCalled = false;
        // if we finish, lets schedule
        CommunicationWorker[] workers = scheduleWaitFor(nodeMap);
        this.worker = workers[0];
        taskExecution = false;
      }

      // we progress until all the channel finish
      if (notStopped && finishedInstances.get() != nodeMap.size()) {
        channel.progress();
        worker.runChannelComplete();
        return true;
      }

      return false;
    }

    public void close() {
      if (notStopped) {
        throw new RuntimeException("We need to stop the execution before close");
      }

      if (!cleanUpCalled) {
        BatchSharingExecutor2.this.close(executionPlan, nodeMap);
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
