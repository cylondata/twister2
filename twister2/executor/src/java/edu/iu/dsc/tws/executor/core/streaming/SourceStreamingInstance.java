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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.checkpointing.CheckpointingClient;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.net.tcp.request.BlockingSendException;
import edu.iu.dsc.tws.comms.api.MessageFlags;
import edu.iu.dsc.tws.executor.api.INodeInstance;
import edu.iu.dsc.tws.executor.api.IParallelOperation;
import edu.iu.dsc.tws.executor.core.DefaultOutputCollection;
import edu.iu.dsc.tws.executor.core.ExecutorContext;
import edu.iu.dsc.tws.executor.core.TaskContextImpl;
import edu.iu.dsc.tws.ftolerance.api.LocalFileStateStore;
import edu.iu.dsc.tws.ftolerance.api.SnapshotImpl;
import edu.iu.dsc.tws.ftolerance.api.StateStore;
import edu.iu.dsc.tws.ftolerance.util.CheckpointUtils;
import edu.iu.dsc.tws.proto.checkpoint.Checkpoint;
import edu.iu.dsc.tws.task.api.Closable;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.INode;
import edu.iu.dsc.tws.task.api.ISource;
import edu.iu.dsc.tws.task.api.OutputCollection;
import edu.iu.dsc.tws.task.api.TaskMessage;
import edu.iu.dsc.tws.task.api.checkpoint.Checkpointable;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedulePlan;

public class SourceStreamingInstance implements INodeInstance {

  private static final Logger LOG = Logger.getLogger(SourceStreamingInstance.class.getName());
  /**
   * The actual streamingTask executing
   */
  private ISource streamingTask;

  /**
   * Output will go throuh a single queue
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

  private boolean checkpointable;
  private StateStore stateStore;
  private SnapshotImpl snapshot;

  public SourceStreamingInstance(ISource streamingTask, BlockingQueue<IMessage> outStreamingQueue,
                                 Config config, String tName, int taskId,
                                 int globalTaskId, int tIndex, int parallel,
                                 int wId, Map<String, Object> cfgs, Map<String, String> outEdges,
                                 TaskSchedulePlan taskSchedule,
                                 CheckpointingClient checkpointingClient, String taskGraphName) {
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
    this.snapshot = new SnapshotImpl();

    this.checkpointable = this.streamingTask instanceof Checkpointable;
  }

  public void prepare(Config cfg) {
    outputStreamingCollection = new DefaultOutputCollection(outStreamingQueue);
    TaskContextImpl taskContext = new TaskContextImpl(streamingTaskIndex, taskId,
        globalTaskId, taskName, parallelism, workerId,
        outputStreamingCollection, nodeConfigs, outEdges, taskSchedule);
    streamingTask.prepare(cfg, taskContext);
    if (this.checkpointable) {
      Checkpointable checkpointableTask = (Checkpointable) this.streamingTask;
      checkpointableTask.initSnapshot(this.snapshot);

      this.stateStore = new LocalFileStateStore(); //todo change based on config
      this.stateStore.init(config, this.taskGraphName, String.valueOf(globalTaskId));
      try {
        Checkpoint.ComponentDiscoveryResponse componentDiscoveryResponse
            = this.checkpointingClient.sendDiscoveryMessage(this.taskGraphName, globalTaskId);

        //restore snapshot
        if (componentDiscoveryResponse.getVersion() > 0) {
          CheckpointUtils.restoreSnapshot(this.stateStore,
              componentDiscoveryResponse.getVersion(),
              this.snapshot);
          checkpointableTask.restoreSnapshot(this.snapshot);
        }

      } catch (BlockingSendException e) {
        throw new RuntimeException("Failed to send the discovery message from "
            + taskName + " : " + globalTaskId, e);
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
  }

  /**
   * Execution Method calls the SourceTasks run method to get context
   **/
  public boolean execute() {
    if (outStreamingQueue.size() < lowWaterMark) {
      // lets execute the task
      streamingTask.execute();
    }
    // now check the output queue
    while (!outStreamingQueue.isEmpty()) {
      IMessage message = outStreamingQueue.peek();
      if (message != null) {
        String edge = message.edge();
        IParallelOperation op = outStreamingParOps.get(edge);
        // if we successfully send remove message
        if (op.send(globalTaskId, message, 0)) {
          outStreamingQueue.poll();
          //todo change order. what happens if sendBarrier fails?
          this.storeSnapshot(checkpointVersion++);
          this.sendBarrier(op, checkpointVersion, edge);
        } else {
          // we need to break
          break;
        }
      } else {
        break;
      }
    }

    for (Map.Entry<String, IParallelOperation> e : outStreamingParOps.entrySet()) {
      e.getValue().progress();
    }

    return true;
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

  public boolean sendBarrier(IParallelOperation op, Long bid, String edge) {
    ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);
    buffer.putLong(bid);
    return op.send(this.globalTaskId, new TaskMessage<>(buffer.array(),
        edge, this.globalTaskId), MessageFlags.SYNC_BARRIER);
  }

  public BlockingQueue<IMessage> getOutStreamingQueue() {
    return outStreamingQueue;
  }

  public void registerOutParallelOperation(String edge, IParallelOperation op) {
    outStreamingParOps.put(edge, op);
  }


  public void storeSnapshot(long checkpointID) {
    if (this.checkpointable) {
      try {
        //take the task snapshot
        Checkpointable checkpointableTask = (Checkpointable) this.streamingTask;
        checkpointableTask.takeSnapshot(this.snapshot);

        //update the new version
        this.snapshot.setVersion(checkpointID);

        CheckpointUtils.commitState(
            this.stateStore,
            this.taskGraphName,
            this.globalTaskId,
            this.snapshot,
            this.checkpointingClient,
            (id, wid, msg) -> {
              LOG.info("Checkpoint committed with version : " + checkpointID);
            }
        );
      } catch (IOException e) {
        throw new RuntimeException("Failed to write checkpoint", e);
      }
    }
  }
}
