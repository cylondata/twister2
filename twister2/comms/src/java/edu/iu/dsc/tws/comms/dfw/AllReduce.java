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

import edu.iu.dsc.tws.api.comms.DataFlowOperation;
import edu.iu.dsc.tws.api.comms.LogicalPlan;
import edu.iu.dsc.tws.api.comms.ReduceFunction;
import edu.iu.dsc.tws.api.comms.SingularReceiver;
import edu.iu.dsc.tws.api.comms.channel.TWSChannel;
import edu.iu.dsc.tws.api.comms.messaging.MessageReceiver;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageType;
import edu.iu.dsc.tws.api.comms.packing.MessageSchema;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.comms.dfw.io.allreduce.AllReduceBatchFinalReceiver;
import edu.iu.dsc.tws.comms.dfw.io.allreduce.AllReduceStreamingFinalReceiver;
import edu.iu.dsc.tws.comms.dfw.io.bcast.BcastBatchFinalReceiver;
import edu.iu.dsc.tws.comms.dfw.io.bcast.BcastStreamingFinalReceiver;
import edu.iu.dsc.tws.comms.dfw.io.reduce.ReduceBatchPartialReceiver;
import edu.iu.dsc.tws.comms.dfw.io.reduce.ReduceStreamingPartialReceiver;

public class AllReduce implements DataFlowOperation {
  private static final Logger LOG = Logger.getLogger(AllReduce.class.getName());

  /**
   * The reduce operation
   */
  private MToOneTree reduce;

  /**
   * The broadcast operation
   */
  private TreeBroadcast broadcast;

  // the source tasks
  protected Set<Integer> sources;

  // the destination task
  private Set<Integer> destinations;

  // the partial receiver
  private MessageReceiver partialReceiver;

  // the final receiver
  private SingularReceiver finalReceiver;

  /**
   * The channel
   */
  private TWSChannel channel;

  /**
   * THe middle task
   */
  private int middleTask;

  /**
   * The reduce edge
   */
  private int reduceEdge;

  /**
   * The broadcast edge
   */
  private int broadCastEdge;

  /**
   * The task plan
   */
  private LogicalPlan logicalPlan;

  /**
   * The reduce function
   */
  private ReduceFunction reduceFunction;

  /**
   * Weather streaming mode
   */
  private boolean streaming;
  private MessageSchema messageSchema;

  public AllReduce(Config config, TWSChannel chnl, LogicalPlan instancePlan,
                   Set<Integer> sources, Set<Integer> destination, int middleTask,
                   ReduceFunction reduceFn,
                   SingularReceiver finalRecv, MessageType t,
                   int redEdge, int broadEdge,
                   boolean strm, MessageSchema messageSchema) {
    this.channel = chnl;
    this.sources = sources;
    this.destinations = destination;
    this.finalReceiver = finalRecv;
    this.reduceEdge = redEdge;
    this.broadCastEdge = broadEdge;
    this.middleTask = middleTask;
    this.reduceFunction = reduceFn;
    this.streaming = strm;
    this.messageSchema = messageSchema;
    init(config, t, instancePlan);
  }

  private void init(Config config, MessageType t, LogicalPlan instancePlan) {
    this.logicalPlan = instancePlan;

    MessageReceiver finalRcvr;
    if (streaming) {
      finalRcvr = new BcastStreamingFinalReceiver(finalReceiver);
    } else {
      finalRcvr = new BcastBatchFinalReceiver(finalReceiver);
    }
    broadcast = new TreeBroadcast(channel, middleTask, destinations, finalRcvr);
    broadcast.init(config, t, instancePlan, broadCastEdge);

    MessageReceiver receiver;
    if (streaming) {
      this.partialReceiver = new ReduceStreamingPartialReceiver(middleTask, reduceFunction);
      receiver = new AllReduceStreamingFinalReceiver(reduceFunction, broadcast);
    } else {
      this.partialReceiver = new ReduceBatchPartialReceiver(middleTask, reduceFunction);
      receiver = new AllReduceBatchFinalReceiver(reduceFunction, broadcast);
    }

    reduce = new MToOneTree(channel, sources, middleTask,
        receiver, partialReceiver, this.messageSchema);
    reduce.init(config, t, instancePlan, reduceEdge);
  }

  @Override
  public boolean sendPartial(int source, Object message, int flags) {
    return reduce.sendPartial(source, message, flags);
  }

  @Override
  public boolean send(int source, Object message, int flags) {
    return reduce.send(source, message, flags);
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
      boolean reduceProgress = reduce.progress();
      return bCastProgress || reduceProgress;
    } catch (Throwable t) {
      LOG.log(Level.SEVERE, "un-expected error", t);
      throw new RuntimeException(t);
    }
  }

  public boolean isComplete() {
    return reduce.isComplete() && broadcast.isComplete();
  }

  @Override
  public void close() {
    reduce.close();
    broadcast.close();
  }

  @Override
  public void reset() {
    if (partialReceiver != null) {
      partialReceiver.clean();
    }

    if (reduce != null) {
      reduce.reset();
    }

    if (broadcast != null) {
      broadcast.reset();
    }
  }


  @Override
  public void finish(int source) {
    reduce.finish(source);
  }

  @Override
  public LogicalPlan getLogicalPlan() {
    return logicalPlan;
  }

  @Override
  public String getUniqueId() {
    return String.valueOf(reduceEdge);
  }
}
