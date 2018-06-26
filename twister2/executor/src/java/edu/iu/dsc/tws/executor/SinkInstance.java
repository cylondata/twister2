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
package edu.iu.dsc.tws.executor;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.executor.comm.IParallelOperation;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.ISink;
import edu.iu.dsc.tws.task.api.TaskContext;

public class SinkInstance  implements INodeInstance {
  /**
   * The actual task executing
   */
  private ISink task;

  /**
   * All the inputs will come through a single queue, otherwise we need to look
   * at different queues for messages
   */
  private BlockingQueue<IMessage> inQueue;

  /**
   * Inward parallel operations
   */
  private Map<String, IParallelOperation> inParOps = new HashMap<>();

  /**
   * The configuration
   */
  private Config config;

  /**
   * The globally unique task id
   */
  private int taskId;

  /**
   * Task index that goes from 0 to parallism - 1
   */
  private int taskIndex;

  /**
   * Number of parallel tasks
   */
  private int parallelism;

  /**
   * Name of the task
   */
  private String taskName;

  /**
   * Node configurations
   */
  private Map<String, Object> nodeConfigs;

  /**
   * The worker id
   */
  private int workerId;

  public SinkInstance(ISink task, BlockingQueue<IMessage> inQueue, Config config,
                      int tId, int tIndex, int parallel, int wId, Map<String, Object> cfgs) {
    this.task = task;
    this.inQueue = inQueue;
    this.config = config;
    this.taskId = tId;
    this.taskIndex = tIndex;
    this.parallelism = parallel;
    this.nodeConfigs = cfgs;
    this.workerId = wId;

  }

  public void prepare() {
    task.prepare(config, new TaskContext(taskIndex, taskId, taskName,
        parallelism, workerId, nodeConfigs));
  }

  public void execute() {
    while (!inQueue.isEmpty()) {
      IMessage m = inQueue.poll();
      task.execute(m);
    }

    for (Map.Entry<String, IParallelOperation> e : inParOps.entrySet()) {
      e.getValue().progress();
    }
  }

  public void registerInParallelOperation(String edge, IParallelOperation op) {
    inParOps.put(edge, op);
  }

  public BlockingQueue<IMessage> getInQueue() {
    return inQueue;
  }
}
