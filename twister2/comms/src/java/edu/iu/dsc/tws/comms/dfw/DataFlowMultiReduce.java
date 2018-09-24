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

public class DataFlowMultiReduce implements DataFlowOperation {
  private static final Logger LOG = Logger.getLogger(DataFlowMultiReduce.class.getName());
  // the source tasks
  protected Set<Integer> sources;

  // the destination task
  private Set<Integer> destinations;

  // one reduce for each destination
  private Map<Integer, DataFlowReduce> reduceMap;

  // the partial receiver
  private MultiMessageReceiver partialReceiver;

  // the final receiver
  private MultiMessageReceiver finalReceiver;

  private TWSChannel channel;

  private Set<Integer> edges;

  private int executor;

  private TaskPlan taskPlan;

  private MessageType dataType;

  private MessageType keyType;

  public DataFlowMultiReduce(TWSChannel chnl,
                             Set<Integer> sources, Set<Integer> destination,
                             MultiMessageReceiver finalRecv,
                             MultiMessageReceiver partialRecv, Set<Integer> es,
                             MessageType kType, MessageType dType) {
    this.channel = chnl;
    this.sources = sources;
    this.destinations = destination;
    this.partialReceiver = partialRecv;
    this.finalReceiver = finalRecv;
    this.edges = es;
    this.reduceMap = new HashMap<>();
    this.keyType = kType;
    this.dataType = dType;
  }

  @Override
  public boolean send(int source, Object message, int flags) {
    throw new RuntimeException("Not implemented");
  }

  @Override
  public boolean send(int source, Object message, int flags, int target) {
    DataFlowReduce reduce = reduceMap.get(target);
    if (reduce == null) {
      throw new RuntimeException("Un-expected destination: " + target);
    }
    return reduce.send(source, message, flags, target);
  }

  @Override
  public boolean sendPartial(int source, Object message, int flags, int target) {
    DataFlowReduce reduce = reduceMap.get(target);
    if (reduce == null) {
      throw new RuntimeException("Un-expected destination: " + target);
    }
    return reduce.sendPartial(source, message, flags, target);
  }

  @Override
  public synchronized boolean progress() {
    boolean needsFurther = false;
    try {
      for (DataFlowReduce reduce : reduceMap.values()) {
        if (reduce.progress()) {
          needsFurther = true;
        }
      }
      if (finalReceiver.progress()) {
        needsFurther = true;
      }
      if (partialReceiver.progress()) {
        needsFurther = true;
      }
    } catch (Throwable t) {
      LOG.log(Level.SEVERE, "un-expected error", t);
      throw new RuntimeException(t);
    }
    return needsFurther;
  }

  @Override
  public boolean isDelegeteComplete() {
    boolean isDone = true;
    for (DataFlowReduce reduce : reduceMap.values()) {
      isDone = isDone && reduce.isDelegeteComplete();
      if (!isDone) {
        //No need to check further if we already have one false
        return false;
      }
    }
    return isDone;
  }

  @Override
  public boolean isComplete() {
    boolean isDone = true;

    for (DataFlowReduce reduce : reduceMap.values()) {
      isDone = isDone && reduce.isComplete();
      if (!isDone) {
        //No need to check further if we already have one false
        return false;
      }
    }
    return isDone;
  }

  @Override
  public void close() {
  }

  @Override
  public void finish(int source) {
    for (DataFlowReduce reduce : reduceMap.values()) {
      reduce.finish(source);
    }
  }

  @Override
  public TaskPlan getTaskPlan() {
    return taskPlan;
  }

  /**
   * Initialize
   */
  public void init(Config config, MessageType type, TaskPlan instancePlan) {
    executor = instancePlan.getThisExecutor();
    this.taskPlan = instancePlan;
    this.dataType = type;

    Map<Integer, Map<Integer, List<Integer>>> partialReceives = new HashMap<>();
    Map<Integer, Map<Integer, List<Integer>>> finalReceives = new HashMap<>();
    List<Integer> edgeList = new ArrayList<>(edges);
    Collections.sort(edgeList);
    int count = 0;
    for (int dest : destinations) {
      ReducePartialReceiver partialRcvr = new ReducePartialReceiver(dest);
      ReduceFinalReceiver finalRcvr = new ReduceFinalReceiver(dest);
      DataFlowReduce reduce = new DataFlowReduce(channel, sources, dest,
          finalRcvr, partialRcvr, count, dest, true, keyType, dataType);
      reduce.init(config, type, instancePlan, edgeList.get(count));
      reduceMap.put(dest, reduce);
      count++;

      Map<Integer, List<Integer>> expectedTaskIds = reduce.receiveExpectedTaskIds();
      partialReceives.put(dest, expectedTaskIds);
      finalReceives.put(dest, expectedTaskIds);
    }

    finalReceiver.init(config, this, finalReceives);
    partialReceiver.init(config, this, partialReceives);
  }

  @Override
  public boolean sendPartial(int source, Object message, int flags) {
    // now what we need to do
    throw new RuntimeException("Not implemented");
  }

  @Override
  public MessageType getKeyType() {
    return keyType;
  }

  @Override
  public MessageType getDataType() {
    return dataType;
  }

  private class ReducePartialReceiver implements MessageReceiver {
    private int destination;

    ReducePartialReceiver(int dst) {
      this.destination = dst;
    }

    @Override
    public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    }

    @Override
    public boolean onMessage(int source, int path, int target, int flags, Object object) {
//      LOG.info(String.format("%d received message %d %d %d %d",
//          executor, path, target, source, flags));
      return partialReceiver.onMessage(source, this.destination, target, flags, object);
    }

    public boolean progress() {
      return partialReceiver.progress();
    }
  }

  private class ReduceFinalReceiver implements MessageReceiver {
    private int destination;

    ReduceFinalReceiver(int dest) {
      this.destination = dest;
    }

    @Override
    public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    }

    @Override
    public boolean onMessage(int source, int path, int target, int flags, Object object) {
//      LOG.info(String.format("%d received message %d %d %d %d",
//          executor, path, target, source, flags));
      return finalReceiver.onMessage(source, this.destination, target, flags, object);
    }

    @Override
    public boolean progress() {
      return finalReceiver.progress();
    }
  }
}
