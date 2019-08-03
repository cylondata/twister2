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
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.checkpointing.CheckpointingClient;
import edu.iu.dsc.tws.api.checkpointing.StateStore;
import edu.iu.dsc.tws.api.comms.messaging.MessageFlags;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.task.IMessage;
import edu.iu.dsc.tws.api.task.OutputCollection;
import edu.iu.dsc.tws.api.task.TaskMessage;
import edu.iu.dsc.tws.api.task.executor.ExecutorContext;
import edu.iu.dsc.tws.api.task.executor.INodeInstance;
import edu.iu.dsc.tws.api.task.executor.IParallelOperation;
import edu.iu.dsc.tws.api.task.executor.ISync;
import edu.iu.dsc.tws.api.task.graph.OperationMode;
import edu.iu.dsc.tws.api.task.modifiers.Closable;
import edu.iu.dsc.tws.api.task.nodes.ICompute;
import edu.iu.dsc.tws.api.task.nodes.INode;
import edu.iu.dsc.tws.api.task.schedule.elements.TaskSchedulePlan;
import edu.iu.dsc.tws.checkpointing.api.SnapshotImpl;
import edu.iu.dsc.tws.checkpointing.task.CheckpointableTask;
import edu.iu.dsc.tws.checkpointing.util.CheckpointUtils;
import edu.iu.dsc.tws.checkpointing.util.CheckpointingConfigurations;
import edu.iu.dsc.tws.executor.core.DefaultOutputCollection;
import edu.iu.dsc.tws.executor.core.TaskCheckpointUtils;
import edu.iu.dsc.tws.executor.core.TaskContextImpl;

/**
 * The class represents the instance of the executing task
 */
public class TaskStreamingInstance implements INodeInstance, ISync {
  /**
   * The actual task executing
   */
  protected ICompute task;

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
  protected Map<String, IParallelOperation> outParOps = new HashMap<>();

  /**
   * Inward parallel operations
   */
  protected Map<String, IParallelOperation> inParOps = new HashMap<>();

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
  protected Map<String, Set<String>> inputEdges;
  private boolean checkpointable;
  private StateStore stateStore;
  private SnapshotImpl snapshot;

  /**
   * Keep an array for iteration
   */
  private IParallelOperation[] intOpArray;

  /**
   * Keep an array out out edges for iteration
   */
  private String[] inEdgeArray;

  /**
   * Keep an array for iteration
   */
  private IParallelOperation[] outOpArray;

  /**
   * Keep an array out out edges for iteration
   */
  private String[] outEdgeArray;
  private PendingCheckpoint pendingCheckpoint;

  public TaskStreamingInstance(ICompute task, BlockingQueue<IMessage> inQueue,
                               BlockingQueue<IMessage> outQueue, Config config, String tName,
                               int taskId, int globalTaskId, int tIndex,
                               int parallel, int wId, Map<String, Object> cfgs,
                               Map<String, Set<String>> inEdges, Map<String, String> outEdges,
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

  /**
   * Preparing the task
   *
   * @param cfg configuration
   */
  public void prepare(Config cfg) {
    outputCollection = new DefaultOutputCollection(outQueue);
    task.prepare(cfg, new TaskContextImpl(taskIndex, taskId, globalTaskId, taskName, parallelism,
        workerId, outputCollection, nodeConfigs, inputEdges, outputEdges, taskSchedule,
        OperationMode.STREAMING));

    /// we will use this array for iteration
    this.outOpArray = new IParallelOperation[outParOps.size()];
    int index = 0;
    for (Map.Entry<String, IParallelOperation> e : outParOps.entrySet()) {
      this.outOpArray[index++] = e.getValue();
    }

    this.outEdgeArray = new String[outputEdges.size()];
    index = 0;
    for (String e : outputEdges.keySet()) {
      this.outEdgeArray[index++] = e;
    }

    /// we will use this array for iteration
    this.intOpArray = new IParallelOperation[inParOps.size()];
    index = 0;
    for (Map.Entry<String, IParallelOperation> e : inParOps.entrySet()) {
      this.intOpArray[index++] = e.getValue();
    }

    this.inEdgeArray = new String[inputEdges.size()];
    index = 0;
    for (String e : inputEdges.keySet()) {
      this.inEdgeArray[index++] = e;
    }

    if (this.checkpointable) {
      this.stateStore = CheckpointUtils.getStateStore(config);
      this.stateStore.init(config, this.taskGraphName, String.valueOf(globalTaskId));

      this.pendingCheckpoint = new PendingCheckpoint(
          this.taskGraphName,
          (CheckpointableTask) this.task,
          this.globalTaskId,
          this.intOpArray,
          this.inEdgeArray.length,
          this.checkpointingClient,
          this.stateStore,
          this.snapshot
      );

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
   * Executing compute task
   */
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
        // if we successfully send remove
        if (op.send(globalTaskId, message, message.getFlag())) {
          outQueue.poll();
        } else {
          break;
        }
      }
    }

    for (int i = 0; i < outOpArray.length; i++) {
      outOpArray[i].progress();
    }

    for (int i = 0; i < intOpArray.length; i++) {
      intOpArray[i].progress();
    }

    if (this.checkpointable && this.inQueue.isEmpty() && this.outQueue.isEmpty()) {
      long checkpointedBarrierId = this.pendingCheckpoint.execute();
      if (checkpointedBarrierId != -1) {
        this.scheduleBarriers(checkpointedBarrierId);
        ((CheckpointableTask) this.task).onCheckpointPropagated(this.snapshot);
      }
    }

    return true;
  }

  public void scheduleBarriers(Long bid) {
    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
    buffer.putLong(bid);
    for (String edge : outEdgeArray) {
      this.outQueue.add(new TaskMessage<>(buffer.array(),
          MessageFlags.SYNC_BARRIER, edge, this.globalTaskId));
    }
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

      LOG.fine(() -> "Barrier received to " + this.globalTaskId
          + " with id " + barrierId + " from " + edge);
      this.pendingCheckpoint.schedule(edge, barrierId);
    }
    return true;
  }
}
