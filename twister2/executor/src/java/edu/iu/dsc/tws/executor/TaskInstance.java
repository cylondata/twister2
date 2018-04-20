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
   * Parallel operations
   */
  private Map<String, IParallelOperation> outParOps = new HashMap<>();

  /**
   * The edge generator
   */
  private EdgeGenerator edgeGenerator;

  public TaskInstance(ITask task, BlockingQueue<IMessage> inQueue,
                      BlockingQueue<IMessage> outQueue, Config config,
                      EdgeGenerator eGenerator) {
    this.task = task;
    this.inQueue = inQueue;
    this.outQueue = outQueue;
    this.config = config;
    this.edgeGenerator = eGenerator;
  }

  public void prepare() {
    outputCollection = new DefaultOutputCollection(outQueue);

    task.prepare(config, outputCollection);
  }

  public void registerOutParallelOperation(String edge, IParallelOperation op) {
    outParOps.put(edge, op);
  }

  public void execute() {
    while (!inQueue.isEmpty()) {
      IMessage m = inQueue.poll();

      task.run(m);

      // now check the output queue
      while (outQueue.isEmpty()) {
        IMessage message = outQueue.poll();
        if (message != null) {
          String edge = message.edge();

          // invoke the communication operation
          IParallelOperation op = outParOps.get(edge);
          op.send(taskId, message);
        }
      }
    }
  }

  public BlockingQueue<IMessage> getInQueue() {
    return inQueue;
  }

  public BlockingQueue<IMessage> getOutQueue() {
    return outQueue;
  }
}
