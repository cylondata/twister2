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
import edu.iu.dsc.tws.api.compute.IMessage;
import edu.iu.dsc.tws.api.compute.executor.INodeInstance;
import edu.iu.dsc.tws.api.compute.executor.IParallelOperation;
import edu.iu.dsc.tws.api.compute.executor.ISync;
import edu.iu.dsc.tws.api.compute.graph.OperationMode;
import edu.iu.dsc.tws.api.compute.modifiers.Closable;
import edu.iu.dsc.tws.api.compute.nodes.ICompute;
import edu.iu.dsc.tws.api.compute.nodes.INode;
import edu.iu.dsc.tws.api.compute.schedule.elements.TaskSchedulePlan;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.checkpointing.api.SnapshotImpl;
import edu.iu.dsc.tws.checkpointing.task.CheckpointableTask;
import edu.iu.dsc.tws.checkpointing.task.CheckpointingSGatherSink;
import edu.iu.dsc.tws.checkpointing.util.CheckpointUtils;
import edu.iu.dsc.tws.checkpointing.util.CheckpointingConfigurations;
import edu.iu.dsc.tws.executor.core.TaskCheckpointUtils;
import edu.iu.dsc.tws.executor.core.TaskContextImpl;

public class SinkStreamingInstance implements INodeInstance, ISync {

  private static final Logger LOG = Logger.getLogger(SinkStreamingInstance.class.getName());

  private final boolean checkpointable;

  /**
   * The actual streamingTask executing
   */
  protected ICompute streamingTask;

  /**
   * All the inputs will come through a single queue, otherwise we need to look
   * at different queues for messages
   */
  protected BlockingQueue<IMessage> streamingInQueue;

  /**
   * Inward parallel operations
   */
  protected Map<String, IParallelOperation> streamingInParOps = new HashMap<>();

  /**
   * The configuration
   */
  protected Config config;

  /**
   * The globally unique streamingTask id
   */
  protected int globalTaskId;

  /**
   * The task id
   */
  protected int taskId;

  /**
   * Task index that goes from 0 to parallism - 1
   */
  protected int streamingTaskIndex;

  /**
   * Number of parallel tasks
   */
  protected int parallelism;

  /**
   * Name of the streamingTask
   */
  protected String taskName;

  /**
   * Node configurations
   */
  protected Map<String, Object> nodeConfigs;

  /**
   * The worker id
   */
  protected int workerId;

  /**
   * The input edges
   */
  protected Map<String, Set<String>> inEdges;
  protected TaskSchedulePlan taskSchedulePlan;

  private CheckpointingClient checkpointingClient;
  private String taskGraphName;
  private Long taskVersion;
  private StateStore stateStore;
  private SnapshotImpl snapshot;
  private PendingCheckpoint pendingCheckpoint;

  /**
   * Keep an array for iteration
   */
  private IParallelOperation[] intOpArray;

  /**
   * Keep an array out out edges for iteration
   */
  private String[] inEdgeArray;

  private TaskContextImpl taskContext;


  public SinkStreamingInstance(ICompute streamingTask, BlockingQueue<IMessage> streamingInQueue,
                               Config config, String tName, int taskId,
                               int globalTaskID, int tIndex, int parallel,
                               int wId, Map<String, Object> cfgs, Map<String, Set<String>> inEdges,
                               TaskSchedulePlan taskSchedulePlan,
                               CheckpointingClient checkpointingClient,
                               String taskGraphName, Long taskVersion) {
    this.streamingTask = streamingTask;
    this.streamingInQueue = streamingInQueue;
    this.taskId = taskId;
    this.config = config;
    this.globalTaskId = globalTaskID;
    this.streamingTaskIndex = tIndex;
    this.parallelism = parallel;
    this.nodeConfigs = cfgs;
    this.workerId = wId;
    this.taskName = tName;
    this.inEdges = inEdges;
    this.taskSchedulePlan = taskSchedulePlan;
    this.checkpointingClient = checkpointingClient;
    this.taskGraphName = taskGraphName;
    this.taskVersion = taskVersion;
    this.checkpointable = this.streamingTask instanceof CheckpointableTask
        && CheckpointingConfigurations.isCheckpointingEnabled(config);
    this.snapshot = new SnapshotImpl();

  }

  /**
   * Preparing sink task
   *
   * @param cfg configuration
   */
  public void prepare(Config cfg) {
    taskContext = new TaskContextImpl(streamingTaskIndex, taskId,
        globalTaskId, taskName, parallelism, workerId, nodeConfigs, inEdges, taskSchedulePlan,
        OperationMode.STREAMING);
    streamingTask.prepare(cfg, taskContext);

    /// we will use this array for iteration
    this.intOpArray = new IParallelOperation[streamingInParOps.size()];
    int index = 0;
    for (Map.Entry<String, IParallelOperation> e : streamingInParOps.entrySet()) {
      this.intOpArray[index++] = e.getValue();
    }

    this.inEdgeArray = new String[inEdges.size()];
    index = 0;
    for (String e : inEdges.keySet()) {
      this.inEdgeArray[index++] = e;
    }

    if (this.checkpointable) {
      this.stateStore = CheckpointUtils.getStateStore(config);
      this.stateStore.init(config, this.taskGraphName, String.valueOf(globalTaskId));

      this.pendingCheckpoint = new PendingCheckpoint(
          this.taskGraphName,
          (CheckpointableTask) this.streamingTask,
          this.globalTaskId,
          this.intOpArray,
          this.inEdges.size(),
          this.checkpointingClient,
          this.stateStore,
          this.snapshot
      );

      TaskCheckpointUtils.restore(
          (CheckpointableTask) this.streamingTask,
          this.snapshot,
          this.stateStore,
          this.taskVersion,
          globalTaskId
      );
    }
  }

  @Override
  public int getIndex() {
    return this.streamingTaskIndex;
  }

  public boolean execute() {
    while (!streamingInQueue.isEmpty()) {
      IMessage m = streamingInQueue.poll();
      if (m != null) {
        streamingTask.execute(m);
      }
    }

    for (int i = 0; i < intOpArray.length; i++) {
      intOpArray[i].progress();
    }

    if (this.checkpointable && this.streamingInQueue.isEmpty()) {
      long checkpointedBarrierId = this.pendingCheckpoint.execute();
      if (checkpointedBarrierId != -1) {
        ((CheckpointableTask) this.streamingTask).onCheckpointPropagated(this.snapshot);
        taskContext.write(CheckpointingSGatherSink.FT_GATHER_EDGE, checkpointedBarrierId);
      }
    }

    return false;
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

  public void registerInParallelOperation(String edge, IParallelOperation op) {
    streamingInParOps.put(edge, op);
  }

  public BlockingQueue<IMessage> getStreamingInQueue() {
    return streamingInQueue;
  }

  @Override
  public boolean sync(String edge, byte[] value) {
    if (this.checkpointable) {
      ByteBuffer wrap = ByteBuffer.wrap(value);
      long barrierId = wrap.getLong();
      LOG.fine(() -> "Barrier received to " + this.globalTaskId
          + " with id " + barrierId + " from " + edge);
      this.pendingCheckpoint.schedule(edge, barrierId);
    } else {
      streamingInParOps.get(edge).reset();
    }
    return true;
  }
}
