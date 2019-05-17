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
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.checkpointmanager.utils.CheckpointContext;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.executor.api.INodeInstance;
import edu.iu.dsc.tws.executor.api.IParallelOperation;
import edu.iu.dsc.tws.executor.api.ISync;
import edu.iu.dsc.tws.executor.core.TaskContextImpl;
import edu.iu.dsc.tws.ftolerance.api.Snapshot;
import edu.iu.dsc.tws.task.api.Closable;
import edu.iu.dsc.tws.task.api.ICompute;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.INode;
import edu.iu.dsc.tws.task.api.checkpoint.Checkpointable;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedulePlan;

public class SinkStreamingInstance implements INodeInstance, ISync {

  private static final Logger LOG = Logger.getLogger(SinkStreamingInstance.class.getName());

  /**
   * The actual streamingTask executing
   */
  private ICompute streamingTask;

  /**
   * All the inputs will come through a single queue, otherwise we need to look
   * at different queues for messages
   */
  private BlockingQueue<IMessage> streamingInQueue;

  /**
   * Inward parallel operations
   */
  private Map<String, IParallelOperation> streamingInParOps = new HashMap<>();

  /**
   * The configuration
   */
  private Config config;

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
   * The worker id
   */
  private int workerId;

  /**
   * The input edges
   */
  private Map<String, String> inEdges;
  private TaskSchedulePlan taskSchedulePlan;

  public SinkStreamingInstance(ICompute streamingTask, BlockingQueue<IMessage> streamingInQueue,
                               Config config, String tName, int taskId,
                               int globalTaskID, int tIndex, int parallel,
                               int wId, Map<String, Object> cfgs, Map<String, String> inEdges,
                               TaskSchedulePlan taskSchedulePlan) {
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
    if (CheckpointContext.getCheckpointRecovery(config)) {
      try {
        LocalStreamingStateBackend fsStateBackend = new LocalStreamingStateBackend();
        Snapshot snapshot = (Snapshot) fsStateBackend.readFromStateBackend(config,
            globalTaskID, workerId);
        ((Checkpointable) this.streamingTask).restoreSnapshot(snapshot);
      } catch (Exception e) {
        LOG.log(Level.WARNING, "Could not read checkpoint", e);
      }
    }
  }

  public void prepare(Config cfg) {
    streamingTask.prepare(cfg, new TaskContextImpl(streamingTaskIndex, taskId,
        globalTaskId, taskName, parallelism, workerId, nodeConfigs, inEdges, taskSchedulePlan));
  }

  public boolean execute() {
    while (!streamingInQueue.isEmpty()) {
      IMessage m = streamingInQueue.poll();
      if (m != null) {
        streamingTask.execute(m);
      }
    }

    for (Map.Entry<String, IParallelOperation> e : streamingInParOps.entrySet()) {
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

  public void registerInParallelOperation(String edge, IParallelOperation op) {
    streamingInParOps.put(edge, op);
  }

  public BlockingQueue<IMessage> getStreamingInQueue() {
    return streamingInQueue;
  }

  public boolean storeSnapshot(int checkpointID) {
    try {
      LocalStreamingStateBackend fsStateBackend = new LocalStreamingStateBackend();
      fsStateBackend.writeToStateBackend(config, globalTaskId, workerId,
          (Checkpointable) streamingTask, checkpointID);
      return true;
    } catch (Exception e) {
      LOG.log(Level.WARNING, "Could not store checkpoint ", e);
      return false;
    }
  }

  @Override
  public boolean sync(String edge, byte[] value) {
    ByteBuffer wrap = ByteBuffer.wrap(value);
    long barrierId = wrap.getLong();

    LOG.info("Sync Received on edge : " + edge + " " + barrierId);

    this.storeSnapshot((int) barrierId);



    //reset comm, to accept new messages
    this.streamingInParOps.get(edge).reset();
    return true;
  }
}
