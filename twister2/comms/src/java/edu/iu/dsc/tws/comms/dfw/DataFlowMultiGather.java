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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.api.MultiMessageReceiver;
import edu.iu.dsc.tws.comms.api.TWSChannel;
import edu.iu.dsc.tws.comms.core.TaskPlan;

public class DataFlowMultiGather implements DataFlowOperation {
  private static final Logger LOG = Logger.getLogger(DataFlowMultiGather.class.getName());
  // the source tasks
  protected Set<Integer> sources;

  // the destination task
  private Set<Integer> destinations;

  // one reduce for each destination
  private Map<Integer, DataFlowGather> gatherMap;

  // the final receiver
  private MultiMessageReceiver finalReceiver;

  private MultiMessageReceiver partialReceiver;

  private TWSChannel channel;

  private Set<Integer> edges;

  private int executor;

  private TaskPlan plan;

  private MessageType type;

  public DataFlowMultiGather(TWSChannel chnl,
                             Set<Integer> sources, Set<Integer> destination,
                             MultiMessageReceiver finalRecv, Set<Integer> es) {
    this.channel = chnl;
    this.sources = sources;
    this.destinations = destination;
    this.finalReceiver = finalRecv;
    this.edges = es;
    this.gatherMap = new HashMap<>();
  }

  public DataFlowMultiGather(TWSChannel chnl, Set<Integer> sources, Set<Integer> destination,
                             MultiMessageReceiver finalRecv, MultiMessageReceiver partialRecv,
                             Set<Integer> es) {
    this.channel = chnl;
    this.sources = sources;
    this.destinations = destination;
    this.finalReceiver = finalRecv;
    this.edges = es;
    this.gatherMap = new HashMap<>();
    this.partialReceiver = partialRecv;
  }

  @Override
  public boolean send(int source, Object message, int flags) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public boolean send(int source, Object message, int flags, int dest) {
    DataFlowGather gather = gatherMap.get(dest);
    if (gather == null) {
      throw new RuntimeException(String.format("%d Un-expected destination: %d %s",
          executor, dest, gatherMap.keySet()));
    }
    boolean send = gather.send(source, message, flags, dest);
//  LOG.info(String.format("%d sending message on reduce: %d %d %b", executor, path, source, send));
    return send;
  }

  @Override
  public boolean sendPartial(int source, Object message, int flags, int dest) {
    DataFlowGather gather = gatherMap.get(dest);
    if (gather == null) {
      throw new RuntimeException(String.format("%d Un-expected destination: %d %s",
          executor, dest, gatherMap.keySet()));
    }
    boolean send = gather.sendPartial(source, message, flags, dest);
//  LOG.info(String.format("%d sending message on reduce: %d %d %b", executor, path, source, send));
    return send;
  }

  @Override
  public synchronized void progress() {
    try {
      for (DataFlowGather reduce : gatherMap.values()) {
        reduce.progress();
      }
      if (partialReceiver != null) {
        partialReceiver.progress();
      }
      finalReceiver.progress();
    } catch (Throwable t) {
      LOG.log(Level.SEVERE, "un-expected error", t);
      throw new RuntimeException(t);
    }
  }

  @Override
  public void close() {
  }

  @Override
  public void finish(int source) {
  }

  @Override
  public TaskPlan getTaskPlan() {
    return plan;
  }

  /**
   * Initialize
   * @param config
   * @param t
   * @param instancePlan
   * @param edge
   */
  public void init(Config config, MessageType t, TaskPlan instancePlan, int edge) {
    executor = instancePlan.getThisExecutor();
    this.type = t;
    this.plan = instancePlan;

    Map<Integer, Map<Integer, List<Integer>>> finalReceives = new HashMap<>();
    Map<Integer, Map<Integer, List<Integer>>> partialReceives = new HashMap<>();
    List<Integer> edgeList = new ArrayList<>(edges);
    Collections.sort(edgeList);
    int count = 0;
    for (int dest : destinations) {
      GatherFinalReceiver finalRcvr = new GatherFinalReceiver(dest);
      GatherPartialReceiver partialRcvr = null;
      DataFlowGather gather = null;
      if (partialReceiver != null) {
        partialRcvr = new GatherPartialReceiver(dest);
        gather = new DataFlowGather(channel, sources, dest,
            finalRcvr, partialRcvr, count, dest, config, t, instancePlan, edgeList.get(count));
      } else {
        gather = new DataFlowGather(channel, sources, dest,
            finalRcvr, count, dest, config, t, instancePlan, edgeList.get(count));
      }

      gather.init(config, t, instancePlan, edgeList.get(count));
      gatherMap.put(dest, gather);
      count++;

      Map<Integer, List<Integer>> expectedTaskIds = gather.receiveExpectedTaskIds();
//      for (Map.Entry<Integer, List<Integer>> e : expectedTaskIds.entrySet()) {
//        finalReceives.put(e.getKey(), e.getValue());
//        partialReceives.put(e.getKey(), e.getValue());
//      }
      finalReceives.put(dest, expectedTaskIds);
      partialReceives.put(dest, expectedTaskIds);
    }

    finalReceiver.init(config, this, finalReceives);
    if (partialReceiver != null) {
      partialReceiver.init(config, this, partialReceives);
    }
  }

  @Override
  public boolean sendPartial(int source, Object message, int flags) {
    // now what we need to do
    throw new RuntimeException("Not implemented");
  }

  private class GatherPartialReceiver implements MessageReceiver {
    private int destination;

    GatherPartialReceiver(int dest) {
      this.destination = dest;
    }

    @Override
    public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    }

    @Override
    public boolean onMessage(int source, int dest, int target, int flags, Object object) {
//      LOG.info(String.format("%d received message %d %d %d", executor, path, target, source));
      return partialReceiver.onMessage(source, this.destination, target, flags, object);
    }

    @Override
    public void progress() {
    }
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
    public boolean onMessage(int source, int dest, int target, int flags, Object object) {
//      LOG.info(String.format("%d received message %d %d %d", executor, path, target, source));
      return finalReceiver.onMessage(source, this.destination, target, flags, object);
    }

    @Override
    public void progress() {
    }
  }
}
