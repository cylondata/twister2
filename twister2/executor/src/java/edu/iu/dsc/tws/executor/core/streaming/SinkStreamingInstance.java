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

import java.util.ArrayList;
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
import edu.iu.dsc.tws.task.api.ICheckPointable;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.INode;
import edu.iu.dsc.tws.task.api.ISink;
import edu.iu.dsc.tws.task.api.SinkCheckpointableTask;
import edu.iu.dsc.tws.task.api.Snapshot;
import edu.iu.dsc.tws.task.api.TaskContext;

public class SinkStreamingInstance implements INodeInstance {

  private static final Logger LOG = Logger.getLogger(SinkStreamingInstance.class.getName());

  /**
   * The actual streamingTask executing
   */
  private ISink streamingTask;

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
  private int streamingTaskId;

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

  public SinkStreamingInstance(ISink streamingTask, BlockingQueue<IMessage> streamingInQueue,
                               Config config, String tName, int tId, int tIndex, int parallel,
                               int wId, Map<String, Object> cfgs, Set<String> inEdges) {
    this.streamingTask = streamingTask;
    this.streamingInQueue = streamingInQueue;
    this.config = config;
    this.streamingTaskId = tId;
    this.streamingTaskIndex = tIndex;
    this.parallelism = parallel;
    this.nodeConfigs = cfgs;
    this.workerId = wId;
    this.taskName = tName;
    if (CheckpointContext.getCheckpointRecovery(config)) {
      try {
        LocalStreamingStateBackend fsStateBackend = new LocalStreamingStateBackend();
        Snapshot snapshot = (Snapshot) fsStateBackend.readFromStateBackend(config,
            streamingTaskId, workerId);
        ((ICheckPointable) this.streamingTask).restoreSnapshot(snapshot);
      } catch (Exception e) {
        LOG.log(Level.WARNING, "Could not read checkpoint", e);
      }
    }
  }

  public void prepare() {

    TaskContext taskContext = new TaskContext(streamingTaskIndex, streamingTaskId, taskName,
        parallelism, workerId, nodeConfigs);

    streamingTask.prepare(config, taskContext);

    if (streamingTask instanceof SinkCheckpointableTask) {
      ((SinkCheckpointableTask) streamingTask).connect(config, taskContext);
    }
  }

  public boolean execute() {
    while (!streamingInQueue.isEmpty()) {
      IMessage message = streamingInQueue.poll();
      if (message != null) {
        if ((message.getFlag() & MessageFlags.SYNC) != MessageFlags.SYNC) {
          streamingTask.execute(message);
        } else {
          //Send acknowledge message to jobmaster
          LOG.info("Barrier message received in Sink " + streamingTaskId
              + " from source " + message.sourceTask());

          Object messageContent = message.getContent();
          LOG.info("message content : " + (messageContent instanceof Integer));
          if (messageContent instanceof ArrayList) {
            @SuppressWarnings("unchecked")
            ArrayList<Integer> messageArray = (ArrayList<Integer>) messageContent;

            if (storeSnapshot(messageArray.get(0))) {
              ((SinkCheckpointableTask) streamingTask).receivedValidBarrier(message);
            }
          } else if (messageContent instanceof Integer) {
            @SuppressWarnings("unchecked")
            Integer barrierID = (Integer) messageContent;

            if (storeSnapshot(barrierID)) {
              ((SinkCheckpointableTask) streamingTask).receivedValidBarrier(message);
            }
          }
        }
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

  public void registerInParallelOperation(String edge, IParallelOperation op) {
    streamingInParOps.put(edge, op);
  }

  public BlockingQueue<IMessage> getstreamingInQueue() {
    return streamingInQueue;
  }

  public boolean storeSnapshot(int checkpointID) {
    try {
      ((SinkCheckpointableTask) streamingTask).addCheckpointableStates();
      LocalStreamingStateBackend fsStateBackend = new LocalStreamingStateBackend();
      fsStateBackend.writeToStateBackend(config, streamingTaskId, workerId,
          (ICheckPointable) streamingTask, checkpointID);
      return true;
    } catch (Exception e) {
      LOG.log(Level.WARNING, "Could not store checkpoint ", e);
      return false;
    }
  }
}
