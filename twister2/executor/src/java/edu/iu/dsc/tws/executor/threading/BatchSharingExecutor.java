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

import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;

import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.executor.api.INodeInstance;

public class BatchSharingExecutor extends ThreadSharingExecutor {
  /**
   * Execution Method for Batch Tasks
   */
  public boolean runExecution() {
    Map<Integer, INodeInstance> nodes = executionPlan.getNodes();
    tasks = new ArrayBlockingQueue<>(nodes.size() * 2);
    tasks.addAll(nodes.values());

    int curTaskSize = tasks.size();
    BatchWorker[] workers = new BatchWorker[curTaskSize];

    for (int i = 0; i < curTaskSize; i++) {
      workers[i] = new BatchWorker();
      Thread t = new Thread(workers[i]);
      t.setName("Thread-From-ReduceBatchTask : " + i);
      t.start();
      threads.add(t);
    }

    progressBatchComm();

    return isDone();
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
        INodeInstance nodeInstance = tasks.poll();
        if (nodeInstance != null) {
          boolean needsFurther = nodeInstance.execute();

        }
      }
    }
  }
}
