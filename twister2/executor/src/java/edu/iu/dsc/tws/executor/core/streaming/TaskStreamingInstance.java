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
package edu.iu.dsc.tws.executor.core.streaming;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.checkpointmanager.utils.CheckpointContext;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.MessageFlags;
import edu.iu.dsc.tws.executor.api.INodeInstance;
import edu.iu.dsc.tws.executor.api.IParallelOperation;
import edu.iu.dsc.tws.executor.core.DefaultOutputCollection;
import edu.iu.dsc.tws.executor.core.ExecutorContext;
import edu.iu.dsc.tws.task.api.ICompute;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.INode;
import edu.iu.dsc.tws.task.api.OutputCollection;
import edu.iu.dsc.tws.task.api.TaskContext;

/**
 * The class represents the instance of the executing task
 */
public class TaskStreamingInstance implements INodeInstance {
  /**
   * The actual task executing
   */
  private ICompute task;

  private static final Logger LOG = Logger.getLogger(TaskStreamingInstance.class.getName());

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
   * The worker id
   */
  private int workerId;

  /**
   * The low watermark for queued messages
   */
  private int lowWaterMark;

  /**
   * The high water mark for messages
   */
  private int highWaterMark;

  public TaskStreamingInstance(ICompute task, BlockingQueue<IMessage> inQueue,
                               BlockingQueue<IMessage> outQueue, Config config, String tName,
                               int tId, int tIndex, int parallel, int wId, Map<String, Object> cfgs,
                               Set<String> inEdges, Set<String> outEdges) {
    this.task = task;
    this.inQueue = inQueue;
    this.outQueue = outQueue;
    this.config = config;
    this.taskId = tId;
    this.taskIndex = tIndex;
    this.parallelism = parallel;
    this.taskName = tName;
    this.nodeConfigs = cfgs;
    this.workerId = wId;
    this.lowWaterMark = ExecutorContext.instanceQueueLowWaterMark(config);
    this.highWaterMark = ExecutorContext.instanceQueueHighWaterMark(config);
    if (CheckpointContext.getCheckpointRecovery(config)) {
      try {
        LocalStreamingStateBackend fsStateBackend = new LocalStreamingStateBackend();
//        this.task = (ICompute) fsStateBackend.readFromStateBackend(config,
//            taskId, workerId);
      } catch (Exception e) {
        LOG.log(Level.WARNING, "Could not read checkpoint", e);
      }
    }
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

  /**
   * Execution Method calls the SourceTasks run method to get context
   **/
  public boolean execute() {
    // execute if there are incoming messages
    while (!inQueue.isEmpty() && outQueue.size() < lowWaterMark) {
      IMessage message = inQueue.poll();
      if (message != null) {
        if ((message.getFlag() & MessageFlags.SYNC) != MessageFlags.SYNC) {
          task.execute(message);
        } else {
          if (storeSnapshot()) {
            outQueue.add(message);
          }
        }
      }
    }

    // now check the output queue
    while (!outQueue.isEmpty()) {
      IMessage message = outQueue.peek();
      if (message != null) {
        String edge = message.edge();

        // invoke the communication operation
        IParallelOperation op = outParOps.get(edge);
        int flags = 0;
        if ((message.getFlag() & MessageFlags.SYNC) == MessageFlags.SYNC) {
          message.setFlag(MessageFlags.BARRIER);
          flags = MessageFlags.BARRIER;
        }
        // if we successfully send remove
        if (op.send(taskId, message, flags)) {
          outQueue.poll();
        } else {
          break;
        }
      }
    }

    for (Map.Entry<String, IParallelOperation> e : outParOps.entrySet()) {
      e.getValue().progress();
    }

    for (Map.Entry<String, IParallelOperation> e : inParOps.entrySet()) {
      e.getValue().progress();
    }

    return true;
  }

  @Override
  public INode getNode() {
    return task;
  }

  public BlockingQueue<IMessage> getInQueue() {
    return inQueue;
  }

  public BlockingQueue<IMessage> getOutQueue() {
    return outQueue;
  }

  public boolean storeSnapshot() {
    try {
      LocalStreamingStateBackend fsStateBackend = new LocalStreamingStateBackend();
      fsStateBackend.writeToStateBackend(config, taskId, workerId, task);
      return true;
    } catch (Exception e) {
      LOG.log(Level.WARNING, " Could not store checkpoint", e);
      return false;
    }
  }
}
