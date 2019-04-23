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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.TWSChannel;
import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.executor.api.IExecution;
import edu.iu.dsc.tws.executor.api.INodeInstance;
import edu.iu.dsc.tws.executor.api.IParallelOperation;

public class BatchSharingExecutor extends ThreadSharingExecutor {
  private static final Logger LOG = Logger.getLogger(BatchSharingExecutor.class.getName());

  // keep track of finished executions
  private Map<Integer, Boolean> finishedInstances = new ConcurrentHashMap<>();

  private int workerId;

  private boolean notStopped = true;

  private boolean cleanUpCalled = false;

  /**
   * Wait for threads to finsih
   */
  private CountDownLatch doneSignal;

  public BatchSharingExecutor(Config cfg, int workerId, TWSChannel channel) {
    super(cfg, channel);
    this.workerId = workerId;
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

    scheduleExecution(nodes);

    // we progress until all the channel finish
    while (finishedInstances.size() != nodes.size()) {
      channel.progress();
    }

    cleanUp(executionPlan, nodes);
    return true;
  }

  private void scheduleExecution(Map<Integer, INodeInstance> nodes) {
    // initialize finished
    // initFinishedInstances();
    BlockingQueue<INodeInstance> tasks;

    tasks = new ArrayBlockingQueue<>(nodes.size() * 2);
    tasks.addAll(nodes.values());

    int curTaskSize = tasks.size();
    BatchWorker[] workers = new BatchWorker[curTaskSize];

    // prepare the tasks
    for (INodeInstance node : tasks) {
      node.prepare(config);
    }

    doneSignal = new CountDownLatch(curTaskSize);
    for (int i = 0; i < curTaskSize; i++) {
      workers[i] = new BatchWorker(tasks);
      threads.submit(workers[i]);
    }
  }

  private void cleanUp(ExecutionPlan executionPlan, Map<Integer, INodeInstance> nodes) {
    // lets wait for thread to finish
    try {
      doneSignal.await();
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted", e);
    }

    // clean up the instances
    for (INodeInstance node : nodes.values()) {
      node.reset();
    }

    // lets close the operations
    List<IParallelOperation> ops = executionPlan.getParallelOperations();
    for (IParallelOperation op : ops) {
      op.reset();
    }

    // clear the finished instances
    finishedInstances.clear();
    cleanUpCalled = true;
  }

  @Override
  public IExecution runIExecution(ExecutionPlan executionPlan) {
    Map<Integer, INodeInstance> nodes = executionPlan.getNodes();

    if (nodes.size() == 0) {
      LOG.warning(String.format("Worker %d has zero assigned tasks, you may "
          + "have more workers than tasks", workerId));
      return null;
    }

    scheduleExecution(nodes);
    return new BatchExecution(executionPlan, nodes);
  }

  @Override
  public boolean waitFor(ExecutionPlan plan) {
    Map<Integer, INodeInstance> nodes = plan.getNodes();

    if (nodes.size() == 0) {
      LOG.warning(String.format("Worker %d has zero assigned tasks, you may "
          + "have more workers than tasks", workerId));
      return true;
    }

    BlockingQueue<INodeInstance> tasks;

    tasks = new ArrayBlockingQueue<>(nodes.size() * 2);
    tasks.addAll(nodes.values());

    int curTaskSize = tasks.size();
    CommunicationWorker[] workers = new CommunicationWorker[curTaskSize];

    doneSignal = new CountDownLatch(curTaskSize);
    for (int i = 0; i < curTaskSize; i++) {
      workers[i] = new CommunicationWorker(tasks);
      threads.submit(workers[i]);
    }

    // we progress until all the channel finish
    while (finishedInstances.size() != nodes.size() || !channel.isComplete()) {
      channel.progress();
    }

    close(plan, nodes);

    return true;
  }

  private void close(ExecutionPlan executionPlan, Map<Integer, INodeInstance> nodes) {
    // lets wait for thread to finish
    try {
      doneSignal.await();
    } catch (InterruptedException e) {
      throw new RuntimeException("Interrupted", e);
    }

    // clean up the instances
    for (INodeInstance node : nodes.values()) {
      node.close();
    }

    // lets close the operations
    List<IParallelOperation> ops = executionPlan.getParallelOperations();
    for (IParallelOperation op : ops) {
      op.close();
    }

    // clear the finished instances
    finishedInstances.clear();
    cleanUpCalled = true;
  }

  protected class CommunicationWorker implements Runnable {
    private BlockingQueue<INodeInstance> tasks;

    public CommunicationWorker(BlockingQueue<INodeInstance> tasks) {
      this.tasks = tasks;
    }

    @Override
    public void run() {
      while (notStopped) {
        try {
          INodeInstance nodeInstance = tasks.poll();
          if (nodeInstance != null) {
            boolean complete = nodeInstance.isComplete();
            if (complete) {
              finishedInstances.put(nodeInstance.getId(), true);
            } else {
              // we need to further execute this task
              tasks.offer(nodeInstance);
            }
          } else {
            break;
          }
        } catch (Throwable t) {
          LOG.log(Level.SEVERE, String.format("%d Error in executor", workerId), t);
          throw new RuntimeException("Error occurred in execution of task", t);
        }
      }
      doneSignal.countDown();
    }
  }

  protected class BatchWorker implements Runnable {
    private BlockingQueue<INodeInstance> tasks;

    public BatchWorker(BlockingQueue<INodeInstance> tasks) {
      this.tasks = tasks;
    }

    @Override
    public void run() {
      while (notStopped) {
        try {
          INodeInstance nodeInstance = tasks.poll();
          if (nodeInstance != null) {
            boolean needsFurther = nodeInstance.execute();
            if (!needsFurther) {
              finishedInstances.put(nodeInstance.getId(), true);
            } else {
              // we need to further execute this task
              tasks.offer(nodeInstance);
            }
          } else {
            break;
          }
        } catch (Throwable t) {
          LOG.log(Level.SEVERE, String.format("%d Error in executor", workerId), t);
          throw new RuntimeException("Error occurred in execution of task", t);
        }
      }
      doneSignal.countDown();
    }
  }

  private class BatchExecution implements IExecution {
    private Map<Integer, INodeInstance> nodeMap;

    private ExecutionPlan executionPlan;

    BatchExecution(ExecutionPlan executionPlan, Map<Integer, INodeInstance> nodeMap) {
      this.nodeMap = nodeMap;
      this.executionPlan = executionPlan;
    }

    @Override
    public boolean waitForCompletion() {
      // we progress until all the channel finish
      while (notStopped && finishedInstances.size() != nodeMap.size()) {
        channel.progress();
      }

      cleanUp(executionPlan, nodeMap);
      return true;
    }

    @Override
    public boolean progress() {
      // we progress until all the channel finish
      if (notStopped && finishedInstances.size() != nodeMap.size()) {
        channel.progress();
        return true;
      }

      return false;
    }

    public void close() {
      if (notStopped) {
        throw new RuntimeException("We need to stop the execution before close");
      }

      if (!cleanUpCalled) {
        cleanUp(executionPlan, nodeMap);
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
