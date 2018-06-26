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
import edu.iu.dsc.tws.task.api.ITask;
import edu.iu.dsc.tws.task.api.OutputCollection;
import edu.iu.dsc.tws.task.api.TaskContext;

/**
 * The class represents the instance of the executing task
 */
public class TaskInstance implements INodeInstance {
  /**
   * The actual task executing
   */
  private ITask task;

  /**
   * All the inputs will come through a single queue, otherwise we need to look
   * at different queues for messages
   */
  private BlockingQueue<IMessage> inQueue;

  /**
   * Output will go throuh a single queue
   */
  private BlockingQueue<IMessage> outQueue;

  /**
   * The configuration
   */
  private Config config;

  /**
   * The output collection to be used
   */
  private OutputCollection outputCollection;

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
   * Parallel operations
   */
  private Map<String, IParallelOperation> outParOps = new HashMap<>();

  /**
   * Inward parallel operations
   */
  private Map<String, IParallelOperation> inParOps = new HashMap<>();

  /**
   * The edge generator
   */
  private EdgeGenerator edgeGenerator;

  /**
   * The worker id
   */
  private int workerId;

  public TaskInstance(ITask task, BlockingQueue<IMessage> inQueue,
                      BlockingQueue<IMessage> outQueue, Config config,
                      EdgeGenerator eGenerator, String tName,
                      int tId, int tIndex, int parallel, int wId, Map<String, Object> cfgs) {
    this.task = task;
    this.inQueue = inQueue;
    this.outQueue = outQueue;
    this.config = config;
    this.edgeGenerator = eGenerator;
    this.taskId = tId;
    this.taskIndex = tIndex;
    this.parallelism = parallel;
    this.taskName = tName;
    this.nodeConfigs = cfgs;
    this.workerId = wId;
  }

  public void prepare() {
    outputCollection = new DefaultOutputCollection(outQueue);
    task.prepare(config, new TaskContext(taskIndex, taskId, taskName, parallelism, workerId,
        outputCollection, nodeConfigs));
  }

  public void registerOutParallelOperation(String edge, IParallelOperation op) {
    outParOps.put(edge, op);
  }

  public void registerInParallelOperation(String edge, IParallelOperation op) {
    inParOps.put(edge, op);
  }

  public void execute() {
    while (!inQueue.isEmpty()) {
      IMessage m = inQueue.poll();

      task.run(m);

      // now check the output queue
      while (!outQueue.isEmpty()) {
        IMessage message = outQueue.poll();
        if (message != null) {
          String edge = message.edge();

          // invoke the communication operation
          IParallelOperation op = outParOps.get(edge);
          op.send(taskId, message);
        }
      }
    }

    for (Map.Entry<String, IParallelOperation> e : outParOps.entrySet()) {
      e.getValue().progress();
    }

    for (Map.Entry<String, IParallelOperation> e : inParOps.entrySet()) {
      e.getValue().progress();
    }
  }

  public BlockingQueue<IMessage> getInQueue() {
    return inQueue;
  }

  public BlockingQueue<IMessage> getOutQueue() {
    return outQueue;
  }
}
