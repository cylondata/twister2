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

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

import edu.iu.dsc.tws.checkpointing.api.SnapshotImpl;
import edu.iu.dsc.tws.checkpointing.api.StateStore;
import edu.iu.dsc.tws.checkpointing.task.CheckpointableTask;
import edu.iu.dsc.tws.checkpointing.util.CheckpointUtils;
import edu.iu.dsc.tws.checkpointing.util.CheckpointingConfigurations;
import edu.iu.dsc.tws.common.checkpointing.CheckpointingClient;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.MessageFlags;
import edu.iu.dsc.tws.executor.api.INodeInstance;
import edu.iu.dsc.tws.executor.api.IParallelOperation;
import edu.iu.dsc.tws.executor.api.ISync;
import edu.iu.dsc.tws.executor.core.DefaultOutputCollection;
import edu.iu.dsc.tws.executor.core.ExecutorContext;
import edu.iu.dsc.tws.executor.core.TaskCheckpointUtils;
import edu.iu.dsc.tws.executor.core.TaskContextImpl;
import edu.iu.dsc.tws.task.api.Closable;
import edu.iu.dsc.tws.task.api.ICompute;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.INode;
import edu.iu.dsc.tws.task.api.OutputCollection;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedulePlan;

/**
 * The class represents the instance of the executing task
 */
public class TaskStreamingInstance implements INodeInstance, ISync {
  /**
   * The actual task executing
   */
  private ICompute task;

  private static final Logger LOG = Logger.getLogger(TaskStreamingInstance.class.getName());

  /**
   * All the inputs will come through a single queue, otherwise we need to look
   * at different queues for messages
   */
  protected BlockingQueue<IMessage> inQueue;

  /**
   * Output will go throuh a single queue
   */
  protected BlockingQueue<IMessage> outQueue;

  /**
   * The configuration
   */
  protected Config config;

  /**
   * The output collection to be used
   */
  protected OutputCollection outputCollection;

  /**
   * The globally unique task id
   */
  protected int globalTaskId;

  /**
   * The task id
   */
  protected int taskId;

  /**
   * Task index that goes from 0 to parallism - 1
   */
  protected int taskIndex;

  /**
   * Number of parallel tasks
   */
  protected int parallelism;

  /**
   * Name of the task
   */
  protected String taskName;

  /**
   * Node configurations
   */
  protected Map<String, Object> nodeConfigs;

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
  protected int workerId;

  /**
   * The low watermark for queued messages
   */
  protected int lowWaterMark;

  /**
   * The high water mark for messages
   */
  protected int highWaterMark;

  /**
   * Output edges
   */
  protected Map<String, String> outputEdges;
  protected TaskSchedulePlan taskSchedule;
  private CheckpointingClient checkpointingClient;
  private String taskGraphName;
  private long tasksVersion;

  /**
   * Input edges
   */
  protected Map<String, String> inputEdges;
  private boolean checkpointable;
  private StateStore stateStore;
  private SnapshotImpl snapshot;

  public TaskStreamingInstance(ICompute task, BlockingQueue<IMessage> inQueue,
                               BlockingQueue<IMessage> outQueue, Config config, String tName,
                               int taskId, int globalTaskId, int tIndex,
                               int parallel, int wId, Map<String, Object> cfgs,
                               Map<String, String> inEdges, Map<String, String> outEdges,
                               TaskSchedulePlan taskSchedule,
                               CheckpointingClient checkpointingClient, String taskGraphName,
                               long tasksVersion) {
    this.task = task;
    this.inQueue = inQueue;
    this.outQueue = outQueue;
    this.config = config;
    this.globalTaskId = globalTaskId;
    this.taskId = taskId;
    this.taskIndex = tIndex;
    this.parallelism = parallel;
    this.taskName = tName;
    this.nodeConfigs = cfgs;
    this.workerId = wId;
    this.lowWaterMark = ExecutorContext.instanceQueueLowWaterMark(config);
    this.highWaterMark = ExecutorContext.instanceQueueHighWaterMark(config);
    this.inputEdges = inEdges;
    this.outputEdges = outEdges;
    this.taskSchedule = taskSchedule;
    this.checkpointingClient = checkpointingClient;
    this.taskGraphName = taskGraphName;
    this.tasksVersion = tasksVersion;
    this.checkpointable = this.task instanceof CheckpointableTask
        && CheckpointingConfigurations.isCheckpointingEnabled(config);
    this.snapshot = new SnapshotImpl();
  }

  public void prepare(Config cfg) {
    outputCollection = new DefaultOutputCollection(outQueue);
    task.prepare(cfg, new TaskContextImpl(taskIndex, taskId, globalTaskId, taskName, parallelism,
        workerId, outputCollection, nodeConfigs, inputEdges, outputEdges, taskSchedule));

    if (this.checkpointable) {
      this.stateStore = CheckpointUtils.getStateStore(config);
      this.stateStore.init(config, this.taskGraphName, String.valueOf(globalTaskId));

      TaskCheckpointUtils.restore(
          (CheckpointableTask) this.task,
          this.snapshot,
          this.stateStore,
          this.tasksVersion,
          globalTaskId
      );
    }

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
      IMessage m = inQueue.poll();
      if (m != null) {
        task.execute(m);
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
        if ((message.getFlag() & MessageFlags.SYNC_BARRIER) == MessageFlags.SYNC_BARRIER) {
          message.setFlag(MessageFlags.SYNC_BARRIER);
          flags = MessageFlags.SYNC_BARRIER;
        }
        // if we successfully send remove
        if (op.send(globalTaskId, message, flags)) {
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

  @Override
  public void close() {
    if (task instanceof Closable) {
      ((Closable) task).close();
    }
  }

  public BlockingQueue<IMessage> getInQueue() {
    return inQueue;
  }

  @Override
  public boolean sync(String edge, byte[] value) {
    if (this.checkpointable) {
      ByteBuffer wrap = ByteBuffer.wrap(value);
      long barrierId = wrap.getLong();

      TaskCheckpointUtils.checkpoint(
          barrierId,
          (CheckpointableTask) this.task,
          this.snapshot,
          this.stateStore,
          this.taskGraphName,
          this.globalTaskId,
          this.checkpointingClient
      );
    }
    return true;
  }
}
