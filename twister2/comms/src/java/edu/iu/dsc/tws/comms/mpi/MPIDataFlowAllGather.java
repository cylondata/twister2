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

import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.api.TWSChannel;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.mpi.io.allgather.AllGatherStreamingFinalReceiver;
import edu.iu.dsc.tws.comms.mpi.io.gather.StreamingPartialGatherReceiver;

public class MPIDataFlowAllGather implements DataFlowOperation {
  private static final Logger LOG = Logger.getLogger(MPIDataFlowAllGather.class.getName());

  private MPIDataFlowGather reduce;

  private MPIDataFlowBroadcast broadcast;
  // the source tasks
  protected Set<Integer> sources;

  // the destination task
  private Set<Integer> destinations;

  // the final receiver
  private MessageReceiver finalReceiver;

  private TWSChannel channel;

  private int executor;

  private int middleTask;

  private int reduceEdge;

  private int broadCastEdge;

  public MPIDataFlowAllGather(TWSChannel chnl,
                              Set<Integer> sources, Set<Integer> destination, int middleTask,
                              MessageReceiver finalRecv,
                              int redEdge, int broadEdge) {
    this.channel = chnl;
    this.sources = sources;
    this.destinations = destination;
    this.finalReceiver = finalRecv;
    this.reduceEdge = redEdge;
    this.broadCastEdge = broadEdge;
    this.middleTask = middleTask;
  }


  /**
   * Initialize
   * @param config
   * @param type
   * @param instancePlan
   * @param edge
   */
  public void init(Config config, MessageType type, TaskPlan instancePlan, int edge) {
    this.executor = instancePlan.getThisExecutor();

    broadcast = new MPIDataFlowBroadcast(channel, middleTask, destinations, finalReceiver);
    broadcast.init(config, type, instancePlan, broadCastEdge);

    StreamingPartialGatherReceiver partialReceiver = new StreamingPartialGatherReceiver();
    AllGatherStreamingFinalReceiver finalRecvr = new AllGatherStreamingFinalReceiver(broadcast);

    reduce = new MPIDataFlowGather(channel, sources, middleTask,
        finalRecvr, partialReceiver, 0, 0, config, type, instancePlan, edge);
    reduce.init(config, type, instancePlan, reduceEdge);
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
      broadcast.progress();
      reduce.progress();
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
    return null;
  }

  @Override
  public TaskPlan getTaskPlan() {
    return null;
  }

  @Override
  public void setMemoryMapped(boolean memoryMapped) {
    reduce.setMemoryMapped(memoryMapped);
    broadcast.setMemoryMapped(memoryMapped);
  }
}
