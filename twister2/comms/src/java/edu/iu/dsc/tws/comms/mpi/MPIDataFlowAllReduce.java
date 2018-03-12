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
package edu.iu.dsc.tws.comms.mpi;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.api.ReduceFunction;
import edu.iu.dsc.tws.comms.api.ReduceReceiver;
import edu.iu.dsc.tws.comms.api.TWSChannel;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.mpi.io.allreduce.AllReduceBatchFinalReceiver;
import edu.iu.dsc.tws.comms.mpi.io.allreduce.AllReduceStreamingFinalReceiver;
import edu.iu.dsc.tws.comms.mpi.io.reduce.ReduceBatchPartialReceiver;
import edu.iu.dsc.tws.comms.mpi.io.reduce.ReduceStreamingPartialReceiver;

public class MPIDataFlowAllReduce implements DataFlowOperation {
  private static final Logger LOG = Logger.getLogger(MPIDataFlowAllReduce.class.getName());

  private MPIDataFlowReduce reduce;

  private MPIDataFlowBroadcast broadcast;
  // the source tasks
  protected Set<Integer> sources;

  // the destination task
  private Set<Integer> destinations;

  // the partial receiver
  private MessageReceiver partialReceiver;

  // the final receiver
  private ReduceReceiver finalReceiver;

  private TWSChannel channel;

  private int executor;

  private int middleTask;

  private int reduceEdge;

  private int broadCastEdge;

  private MessageType type;

  private TaskPlan taskPlan;

  private ReduceFunction reduceFunction;

  private boolean streaming;

  public MPIDataFlowAllReduce(TWSChannel chnl,
                              Set<Integer> sources, Set<Integer> destination, int middleTask,
                              ReduceFunction reduceFn,
                              ReduceReceiver finalRecv,
                              int redEdge, int broadEdge,
                              boolean strm) {
    this.channel = chnl;
    this.sources = sources;
    this.destinations = destination;
    this.finalReceiver = finalRecv;
    this.reduceEdge = redEdge;
    this.broadCastEdge = broadEdge;
    this.middleTask = middleTask;
    this.reduceFunction = reduceFn;
    this.streaming = strm;
  }


  /**
   * Initialize
   * @param config
   * @param t
   * @param instancePlan
   * @param edge
   */
  public void init(Config config, MessageType t, TaskPlan instancePlan, int edge) {
    this.type = t;
    this.executor = instancePlan.getThisExecutor();
    this.taskPlan = instancePlan;
    this.executor = taskPlan.getThisExecutor();

    broadcast = new MPIDataFlowBroadcast(channel, middleTask, destinations,
        new BCastReceiver(finalReceiver));
    broadcast.init(config, t, instancePlan, broadCastEdge);

    MessageReceiver receiver;
    if (streaming) {
      this.partialReceiver = new ReduceStreamingPartialReceiver(middleTask, reduceFunction);
      receiver = new AllReduceStreamingFinalReceiver(reduceFunction, broadcast, middleTask);
    } else {
      this.partialReceiver = new ReduceBatchPartialReceiver(middleTask, reduceFunction);
      receiver = new AllReduceBatchFinalReceiver(reduceFunction, broadcast);
    }

    reduce = new MPIDataFlowReduce(channel, sources, middleTask,
        receiver, partialReceiver);
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
  public boolean send(int source, Object message, int flags, int dest) {
    throw new RuntimeException("Not-implemented");
  }

  @Override
  public boolean sendPartial(int source, Object message, int flags, int dest) {
    throw new RuntimeException("Not-implemented");
  }

  @Override
  public synchronized void progress() {
    try {
      reduce.progress();
      broadcast.progress();
    } catch (Throwable t) {
      LOG.log(Level.SEVERE, "un-expected error", t);
      throw new RuntimeException(t);
    }
  }

  @Override
  public void close() {
  }

  @Override
  public void finish() {
  }

  @Override
  public MessageType getType() {
    return type;
  }

  @Override
  public TaskPlan getTaskPlan() {
    return taskPlan;
  }

  @Override
  public void setMemoryMapped(boolean memoryMapped) {
    reduce.setMemoryMapped(memoryMapped);
    broadcast.setMemoryMapped(memoryMapped);
  }

  private static class BCastReceiver implements MessageReceiver {
    private ReduceReceiver reduceReceiver;

    BCastReceiver(ReduceReceiver reduceRcvr) {
      this.reduceReceiver = reduceRcvr;
    }

    @Override
    public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
      this.reduceReceiver.init(cfg, op, expectedIds);
    }

    @Override
    public boolean onMessage(int source, int path, int target, int flags, Object object) {
      return reduceReceiver.receive(target, object);
    }

    @Override
    public void progress() {
    }
  }
}
