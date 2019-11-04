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
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.checkpointing.CheckpointingClient;
import edu.iu.dsc.tws.api.checkpointing.Snapshot;
import edu.iu.dsc.tws.api.checkpointing.StateStore;
import edu.iu.dsc.tws.api.comms.messaging.MessageFlags;
import edu.iu.dsc.tws.api.compute.IMessage;
import edu.iu.dsc.tws.api.compute.OutputCollection;
import edu.iu.dsc.tws.api.compute.executor.ExecutorContext;
import edu.iu.dsc.tws.api.compute.executor.INodeInstance;
import edu.iu.dsc.tws.api.compute.executor.IParallelOperation;
import edu.iu.dsc.tws.api.compute.graph.OperationMode;
import edu.iu.dsc.tws.api.compute.modifiers.Closable;
import edu.iu.dsc.tws.api.compute.nodes.INode;
import edu.iu.dsc.tws.api.compute.nodes.ISource;
import edu.iu.dsc.tws.api.compute.schedule.elements.TaskSchedulePlan;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.checkpointing.api.SnapshotImpl;
import edu.iu.dsc.tws.checkpointing.task.CheckpointableTask;
import edu.iu.dsc.tws.checkpointing.task.CheckpointingSGatherSink;
import edu.iu.dsc.tws.checkpointing.util.CheckpointUtils;
import edu.iu.dsc.tws.checkpointing.util.CheckpointingConfigurations;
import edu.iu.dsc.tws.executor.core.DefaultOutputCollection;
import edu.iu.dsc.tws.executor.core.TaskCheckpointUtils;
import edu.iu.dsc.tws.executor.core.TaskContextImpl;

public class SourceStreamingInstance implements INodeInstance {

  private static final Logger LOG = Logger.getLogger(SourceStreamingInstance.class.getName());
  /**
   * The actual streamingTask executing
   */
  private ISource streamingTask;

  /**
   * Output will go through a single queue
   */
  private BlockingQueue<IMessage> outStreamingQueue;

  /**
   * The configuration
   */
  private Config config;

  /**
   * The output collection to be used
   */
  private OutputCollection outputStreamingCollection;

  /**
   * Parallel operations
   */
  private Map<String, IParallelOperation> outStreamingParOps = new HashMap<>();

  /**
   * The globally unique streamingTask id
   */
  private int globalTaskId;

  /**
   * The task id
   */
  private int taskId;

  /**
   * Task index that goes from 0 to parallism - 1
   */
  private int streamingTaskIndex;

  /**
   * Number of parallel tasks
   */
  private int parallelism;

  /**
   * Name of the streamingTask
   */
  private String taskName;

  /**
   * Node configurations
   */
  private Map<String, Object> nodeConfigs;

  /**
   * Worker id
   */
  private int workerId;

  /**
   * The low watermark for queued messages
   */
  private int lowWaterMark;

  private long checkpointVersion = 0;

  /**
   * The high water mark for messages
   */
  private int highWaterMark;

  /**
   * The output edges
   */
  private Map<String, String> outEdges;
  private TaskSchedulePlan taskSchedule;
  private CheckpointingClient checkpointingClient;
  private String taskGraphName;
  private long tasksVersion;

  /**
   * Checkpoint variables
   */
  private boolean checkpointable;
  private StateStore stateStore;
  private SnapshotImpl snapshot;
  private int barrierMessagesSent = 0;
  private Queue<Snapshot> snapshotQueue = new LinkedList<>();
  private long executions = 0;
  private long checkPointingFrequency = 1000;

  /**
   * Keep an array for iteration
   */
  private IParallelOperation[] outOpArray;

  /**
   * Keep an array out out edges for iteration
   */
  private String[] outEdgeArray;

  /**
   * The task context implementation
   */
  private TaskContextImpl taskContext;

  /**
   * Keep track of checkpoints
   */
  private PendingCheckpoint pendingCheckpoint;

  public SourceStreamingInstance(ISource streamingTask, BlockingQueue<IMessage> outStreamingQueue,
                                 Config config, String tName, int taskId,
                                 int globalTaskId, int tIndex, int parallel,
                                 int wId, Map<String, Object> cfgs, Map<String, String> outEdges,
                                 TaskSchedulePlan taskSchedule,
                                 CheckpointingClient checkpointingClient, String taskGraphName,
                                 long tasksVersion) {
    this.streamingTask = streamingTask;
    this.taskId = taskId;
    this.outStreamingQueue = outStreamingQueue;
    this.config = config;
    this.globalTaskId = globalTaskId;
    this.streamingTaskIndex = tIndex;
    this.parallelism = parallel;
    this.taskName = tName;
    this.nodeConfigs = cfgs;
    this.workerId = wId;
    this.lowWaterMark = ExecutorContext.instanceQueueLowWaterMark(config);
    this.highWaterMark = ExecutorContext.instanceQueueHighWaterMark(config);
    this.outEdges = outEdges;
    this.taskSchedule = taskSchedule;
    this.checkpointingClient = checkpointingClient;
    this.taskGraphName = taskGraphName;
    this.tasksVersion = tasksVersion;
    this.snapshot = new SnapshotImpl();
    this.checkpointable = this.streamingTask instanceof CheckpointableTask
        && CheckpointingConfigurations.isCheckpointingEnabled(config);
    this.checkPointingFrequency = CheckpointingConfigurations.getCheckPointingFrequency(config);
  }

  public void prepare(Config cfg) {
    outputStreamingCollection = new DefaultOutputCollection(outStreamingQueue);
    taskContext = new TaskContextImpl(streamingTaskIndex, taskId,
        globalTaskId, taskName, parallelism, workerId,
        outputStreamingCollection, nodeConfigs, outEdges, taskSchedule, OperationMode.STREAMING);
    streamingTask.prepare(cfg, taskContext);

    /// we will use this array for iteration
    this.outOpArray = new IParallelOperation[outStreamingParOps.size()];
    int index = 0;
    for (Map.Entry<String, IParallelOperation> e : outStreamingParOps.entrySet()) {
      this.outOpArray[index++] = e.getValue();
    }

    this.outEdgeArray = new String[outEdges.size()];
    index = 0;
    for (String e : outEdges.keySet()) {
      this.outEdgeArray[index++] = e;
    }

    if (this.checkpointable) {
      this.stateStore = CheckpointUtils.getStateStore(config);
      this.stateStore.init(config, this.taskGraphName, String.valueOf(globalTaskId));

      TaskCheckpointUtils.restore(
          (CheckpointableTask) this.streamingTask,
          this.snapshot,
          this.stateStore,
          this.tasksVersion,
          globalTaskId
      );

      this.checkpointVersion = this.tasksVersion + 1;

      this.pendingCheckpoint = new PendingCheckpoint(taskGraphName,
          (CheckpointableTask) this.streamingTask, globalTaskId, outOpArray, outEdges.size(),
          checkpointingClient, stateStore, snapshot);
    }
  }

  @Override
  public int getIndex() {
    return this.streamingTaskIndex;
  }

  /**
   * Execution Method calls the SourceTasks run method to get context
   **/
  public boolean execute() {
    if (outStreamingQueue.size() < lowWaterMark
        && !(this.checkpointable && this.pendingCheckpoint.isPending())) {
      // lets execute the task
      streamingTask.execute();

      if (this.checkpointable && (executions++ % this.checkPointingFrequency) == 0) {
        this.scheduleCheckpoint(checkpointVersion++);
      }
    }
    // we start with setting nothing to execute true
    boolean nothingToProcess = true;
    // now check the output queue
    while (!outStreamingQueue.isEmpty()) {
      IMessage message = outStreamingQueue.peek();
      if (message != null) {
        String edge = message.edge();
        IParallelOperation op = outStreamingParOps.get(edge);
        boolean barrierMessage = (message.getFlag() & MessageFlags.SYNC_BARRIER)
            == MessageFlags.SYNC_BARRIER;
        // if we successfully send remove message
        if (barrierMessage ? op.sendBarrier(globalTaskId, (byte[]) message.getContent())
            : op.send(globalTaskId, message, message.getFlag())) {
          outStreamingQueue.poll();
        } else {
          nothingToProcess = false;
          // we need to break
          break;
        }
      } else {
        break;
      }
    }

    for (int i = 0; i < outOpArray.length; i++) {
      boolean needProgress = outOpArray[i].progress();
      if (needProgress) {
        nothingToProcess = false;
      }
    }

    if (this.checkpointable && outStreamingQueue.isEmpty() && this.pendingCheckpoint.isPending()) {
      long barrier = this.pendingCheckpoint.execute();
      if (barrier != -1) {
        ((CheckpointableTask) this.streamingTask)
            .onCheckpointPropagated(this.snapshot);
        this.scheduleBarriers(barrier);
        taskContext.write(CheckpointingSGatherSink.FT_GATHER_EDGE, barrier);
        nothingToProcess = false;
      }
    }
    return !nothingToProcess;
  }

  @Override
  public INode getNode() {
    return streamingTask;
  }

  @Override
  public void close() {
    if (streamingTask instanceof Closable) {
      ((Closable) streamingTask).close();
    }
  }

  public BlockingQueue<IMessage> getOutStreamingQueue() {
    return outStreamingQueue;
  }

  public void scheduleCheckpoint(Long bid) {
    for (String edge : outStreamingParOps.keySet()) {
      this.pendingCheckpoint.schedule(edge, bid);
    }
  }

  public void scheduleBarriers(Long bid) {
    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
    buffer.putLong(bid);
    for (String edge : outStreamingParOps.keySet()) {
      taskContext.writeBarrier(edge, buffer.array());
    }
  }

  public void registerOutParallelOperation(String edge, IParallelOperation op) {
    outStreamingParOps.put(edge, op);
  }
}
