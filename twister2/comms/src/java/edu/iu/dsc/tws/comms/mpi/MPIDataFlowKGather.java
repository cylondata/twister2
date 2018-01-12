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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.KeyedMessageReceiver;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.core.TaskPlan;

public class MPIDataFlowKGather implements DataFlowOperation {
  // the source tasks
  protected Set<Integer> sources;

  // the destination task
  private Set<Integer> destinations;

  // one reduce for each destination
  private Map<Integer, MPIDataFlowGather> gatherMap;

  // the final receiver
  private KeyedMessageReceiver finalReceiver;

  private TWSMPIChannel channel;

  private Set<Integer> edges;

  private int executor;

  public MPIDataFlowKGather(TWSMPIChannel chnl,
                            Set<Integer> sources, Set<Integer> destination,
                            KeyedMessageReceiver finalRecv, Set<Integer> es) {
    this.channel = chnl;
    this.sources = sources;
    this.destinations = destination;
    this.finalReceiver = finalRecv;
    this.edges = es;
    this.gatherMap = new HashMap<>();
  }

  @Override
  public boolean send(int source, Object message, int flags) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public boolean send(int source, Object message, int flags, int path) {
    MPIDataFlowGather gather = gatherMap.get(path);
    if (gather == null) {
      throw new RuntimeException("Un-expected destination: " + path);
    }
    boolean send = gather.send(source, message, flags, path);
//  LOG.info(String.format("%d sending message on reduce: %d %d %b", executor, path, source, send));
    return send;
  }

  @Override
  public boolean sendPartial(int source, Object message, int path, int flags) {
    MPIDataFlowGather gather = gatherMap.get(path);
    if (gather == null) {
      throw new RuntimeException("Un-expected destination: " + path);
    }
    boolean send = gather.sendPartial(source, message, flags, path);
//  LOG.info(String.format("%d sending message on reduce: %d %d %b", executor, path, source, send));
    return send;
  }

  @Override
  public void progress() {
    for (MPIDataFlowGather reduce : gatherMap.values()) {
      reduce.progress();
    }
    finalReceiver.progress();
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
  public void init(Config config, MessageType type, TaskPlan instancePlan, int edge) {
    executor = instancePlan.getThisExecutor();
    Map<Integer, Map<Integer, List<Integer>>> finalReceives = new HashMap<>();
    List<Integer> edgeList = new ArrayList<>(edges);
    Collections.sort(edgeList);
    int count = 0;
    for (int dest : destinations) {
      GatherFinalReceiver finalRcvr = new GatherFinalReceiver(dest);
      MPIDataFlowGather gather = new MPIDataFlowGather(channel, sources, dest,
          finalRcvr, count, dest);
      gather.init(config, type, instancePlan, edgeList.get(count));
      gatherMap.put(dest, gather);
      count++;

      finalReceives.put(dest, gather.receiveExpectedTaskIds());
    }

    finalReceiver.init(config, this, finalReceives);
  }

  @Override
  public boolean sendPartial(int source, Object message, int flags) {
    // now what we need to do
    throw new RuntimeException("Not implemented");
  }

  private class GatherFinalReceiver implements MessageReceiver {
    private int destination;

    GatherFinalReceiver(int dest) {
      this.destination = dest;
    }

    @Override
    public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    }

    @Override
    public boolean onMessage(int source, int path, int target, int flags, Object object) {
//      LOG.info(String.format("%d received message %d %d %d", executor, path, target, source));
      return finalReceiver.onMessage(source, destination, target, flags, object);
    }

    @Override
    public void progress() {
    }
  }
}
