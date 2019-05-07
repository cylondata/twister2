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
package edu.iu.dsc.tws.comms.dfw;

import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.BulkReceiver;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.api.MessageTypes;
import edu.iu.dsc.tws.comms.api.TWSChannel;
import edu.iu.dsc.tws.comms.api.TaskPlan;
import edu.iu.dsc.tws.comms.dfw.io.Tuple;
import edu.iu.dsc.tws.comms.dfw.io.allgather.AllGatherBatchFinalReceiver;
import edu.iu.dsc.tws.comms.dfw.io.allgather.AllGatherStreamingFinalReceiver;
import edu.iu.dsc.tws.comms.dfw.io.allgather.BcastGatheStreamingReceiver;
import edu.iu.dsc.tws.comms.dfw.io.allgather.BcastGatherBatchReceiver;
import edu.iu.dsc.tws.comms.dfw.io.gather.GatherBatchPartialReceiver;
import edu.iu.dsc.tws.comms.dfw.io.gather.GatherStreamingPartialReceiver;

public class AllGather implements DataFlowOperation {
  private static final Logger LOG = Logger.getLogger(AllGather.class.getName());

  /**
   * Gather operation
   */
  private MToOneTree gather;

  /**
   * Broadcast operation
   */
  private TreeBroadcast broadcast;

  // the source tasks
  protected Set<Integer> sources;

  // the destination task
  private Set<Integer> destinations;

  // the final receiver
  private BulkReceiver finalReceiver;

  /**
   * The channel
   */
  private TWSChannel channel;

  /**
   * The middle task
   */
  private int middleTask;

  /**
   * The edge used for gather
   */
  private int gatherEdge;

  /**
   * The edge used for broadcast
   */
  private int broadCastEdge;

  /**
   * Weather it is streaming
   */
  private boolean streaming;

  /**
   * Data type
   */
  private MessageType dataType;

  /**
   * Task plan
   */
  private TaskPlan taskPlan;

  public AllGather(Config config, TWSChannel chnl, TaskPlan instancePlan,
                   Set<Integer> sources, Set<Integer> destination, int middleTask,
                   BulkReceiver finalRecv, MessageType type,
                   int redEdge, int broadEdge, boolean stream) {
    this.channel = chnl;
    this.sources = sources;
    this.destinations = destination;
    this.finalReceiver = finalRecv;
    this.gatherEdge = redEdge;
    this.broadCastEdge = broadEdge;
    this.middleTask = middleTask;
    this.streaming = stream;
    this.dataType = type;
    this.taskPlan = instancePlan;
    init(config, type, instancePlan);
  }

  private void init(Config config, MessageType type, TaskPlan instancePlan) {
    MessageReceiver finalRcvr;
    if (streaming) {
      finalRcvr = new BcastGatheStreamingReceiver(finalReceiver);
    } else {
      finalRcvr = new BcastGatherBatchReceiver(finalReceiver);
    }
    broadcast = new TreeBroadcast(channel, middleTask,
        destinations, finalRcvr, MessageTypes.INTEGER, type);
    broadcast.init(config, type, instancePlan, broadCastEdge);

    MessageReceiver partialReceiver;
    MessageReceiver finalRecvr;
    if (streaming) {
      finalRecvr = new AllGatherStreamingFinalReceiver(broadcast);
      partialReceiver = new GatherStreamingPartialReceiver();
    } else {
      finalRecvr = new AllGatherBatchFinalReceiver(broadcast);
      partialReceiver = new GatherBatchPartialReceiver(0);
    }

    gather = new MToOneTree(channel, sources, middleTask,
        finalRecvr, partialReceiver, 0, 0, true,
        MessageTypes.INTEGER, type);
    gather.init(config, type, instancePlan, gatherEdge);
  }

  @Override
  public boolean sendPartial(int source, Object message, int flags) {
    return gather.sendPartial(source, message, flags);
  }

  @Override
  public boolean send(int source, Object message, int flags) {
    Tuple tuple = new Tuple<>(source, message, MessageTypes.INTEGER, dataType);
    return gather.send(source, tuple, flags);
  }

  @Override
  public boolean send(int source, Object message, int flags, int target) {
    throw new RuntimeException("Not-implemented");
  }

  @Override
  public boolean sendPartial(int source, Object message, int flags, int target) {
    throw new RuntimeException("Not-implemented");
  }

  @Override
  public synchronized boolean progress() {
    try {
      boolean bCastProgress = broadcast.progress();
      boolean reduceProgress = gather.progress();
      return bCastProgress || reduceProgress;
    } catch (Throwable t) {
      LOG.log(Level.SEVERE, "un-expected error", t);
      throw new RuntimeException(t);
    }
  }

  public boolean isComplete() {
    boolean gatherComplete = gather.isComplete();
    boolean bcastComplete = broadcast.isComplete();
    return gatherComplete && bcastComplete;
  }

  @Override
  public void close() {
    broadcast.close();
    gather.close();
  }

  @Override
  public void clean() {
    if (gather != null) {
      gather.clean();
    }

    if (broadcast != null) {
      broadcast.clean();
    }
  }

  @Override
  public void finish(int source) {
    gather.finish(source);
  }

  @Override
  public TaskPlan getTaskPlan() {
    return taskPlan;
  }

  @Override
  public String getUniqueId() {
    return String.valueOf(gatherEdge);
  }
}
