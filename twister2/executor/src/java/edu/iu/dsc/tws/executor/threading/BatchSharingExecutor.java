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

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.executor.api.INodeInstance;

public class BatchSharingExecutor extends ThreadSharingExecutor {
  private static final Logger LOG = Logger.getLogger(BatchSharingExecutor.class.getName());

  // keep track of finished executions
  private Map<Integer, Boolean> finishedInstances = new HashMap<>();

  private int workerId;

  public BatchSharingExecutor(int workerId) {
    this.workerId = workerId;
  }

  /**
   * Execution Method for Batch Tasks
   */
  public boolean runExecution() {
    Map<Integer, INodeInstance> nodes = executionPlan.getNodes();

    // initialize finished
    // initFinishedInstances();
    tasks = new ArrayBlockingQueue<>(nodes.size() * 2);
    tasks.addAll(nodes.values());

    int curTaskSize = tasks.size();
    BatchWorker[] workers = new BatchWorker[curTaskSize];

    // prepare the tasks
    for (INodeInstance node : tasks) {
      node.prepare();
    }

    for (int i = 0; i < curTaskSize; i++) {
      workers[i] = new BatchWorker();
      Thread t = new Thread(workers[i], "Executor-" + i);
      threads.add(t);
      t.start();
    }

    // we progress until all the channel finish
    while (finishedInstances.size() != nodes.size()) {
      channel.progress();
    }

    // lets wait for thread to finish
    for (Thread t : threads) {
      try {
        t.join();
      } catch (InterruptedException e) {
      }
    }

    return true;
  }

  /**
   * Set all instances finish to false
   */
  private void initFinishedInstances() {
    Map<Integer, INodeInstance> nodes = executionPlan.getNodes();
    for (Integer n : nodes.keySet()) {
      finishedInstances.put(n, false);
    }
  }

  public void progressBatchComm() {
    while (!isDone()) {
      this.channel.progress();
    }
  }

  @Override
  public void stop(ExecutionPlan execution) {

  }

  protected class BatchWorker implements Runnable {
    @Override
    public void run() {
      while (true) {
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
        }
      }
    }
  }
}
