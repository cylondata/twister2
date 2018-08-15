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
import java.util.concurrent.BlockingQueue;

import edu.iu.dsc.tws.executor.api.ExecutionModel;
import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.executor.api.INodeInstance;
import edu.iu.dsc.tws.executor.core.batch.SinkBatchInstance;
import edu.iu.dsc.tws.executor.core.batch.SourceBatchInstance;

public class ThreadSharedExecutor extends ThreadExecutorFactory {

  private int numThreads;

  private BlockingQueue<INodeInstance> tasks;

  private BlockingQueue<INodeInstance> executedTasks;

  private ExecutionPlan executionPlan;

  private int totalTasks;

  public ThreadSharedExecutor(ExecutionModel executionModels,
                              ThreadExecutor executor, ExecutionPlan executionPlan) {
    super(executionModels, executor, executionPlan);
    init(executionPlan);
    prepare();
  }

  public void init(ExecutionPlan plan) {
    this.executionPlan = plan;
  }

  public void prepare() {
    Map<Integer, INodeInstance> nodes = this.executionPlan.getNodes();
    this.tasks = new ArrayBlockingQueue<>(nodes.size() * 2);
    this.tasks.addAll(nodes.values());
    this.totalTasks = tasks.size();
    for (INodeInstance node : tasks) {
      node.prepare();
    }
  }

  @Override
  public boolean execute() {
    boolean isExecuted = false;
    for (int i = 0; i < tasks.size(); i++) {
      INodeInstance iNodeInstance = tasks.poll();

      if (iNodeInstance != null) {
        if (iNodeInstance instanceof SourceBatchInstance) {
          SourceBatchInstance sourceBatchInstance = (SourceBatchInstance) iNodeInstance;
          SourceWorker sourceWorker = new SourceWorker(sourceBatchInstance);
          Thread sourceThread = new Thread(sourceWorker);
          sourceThread.start();
        }

        if (iNodeInstance instanceof SinkBatchInstance) {
          SinkBatchInstance sinkBatchInstance = (SinkBatchInstance) iNodeInstance;
          SinkWorker sinkWorker = new SinkWorker(sinkBatchInstance);
          Thread sinkThread = new Thread(sinkWorker);
          sinkThread.start();
        }

        tasks.offer(iNodeInstance);
      }
    }

    return isExecuted;
  }
}
