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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.core.TaskPlan;

public class MPIDataFlowAllReduce implements DataFlowOperation {
  private static final Logger LOG = Logger.getLogger(MPIDataFlowAllReduce.class.getName());

  private MPIDataFlowReduce reduce;

  private MPIDataFlowBroadcast broadcast;
  // the source tasks
  protected Set<Integer> sources;

  // the destination task
  private Set<Integer> destinations;

  // one reduce for each destination
  private Map<Integer, MPIDataFlowReduce> reduceMap;

  // the partial receiver
  private MessageReceiver partialReceiver;

  // the final receiver
  private MessageReceiver finalReceiver;

  private TWSMPIChannel channel;

  private int executor;

  private int middleTask;

  private int reduceEdge;

  private int broadCastEdge;

  public MPIDataFlowAllReduce(TWSMPIChannel chnl,
                              Set<Integer> sources, Set<Integer> destination, int middleTask,
                              MessageReceiver finalRecv,
                              MessageReceiver partialRecv,
                              int redEdge, int broadEdge) {
    this.channel = chnl;
    this.sources = sources;
    this.destinations = destination;
    this.partialReceiver = partialRecv;
    this.finalReceiver = finalRecv;
    this.reduceMap = new HashMap<>();
    this.reduceEdge = redEdge;
    this.broadCastEdge = broadEdge;
    this.middleTask = middleTask;
  }

  @Override
  public void init(Config config, MessageType type, TaskPlan instancePlan, int edge) {
    this.executor = instancePlan.getThisExecutor();
    ReduceFinalReceiver finalRcvr = new ReduceFinalReceiver();
    reduce = new MPIDataFlowReduce(channel, sources, middleTask,
        finalRcvr, partialReceiver);
    reduce.init(config, type, instancePlan, reduceEdge);
    Map<Integer, List<Integer>> receiveExpects = reduce.receiveExpectedTaskIds();
    finalRcvr.init(receiveExpects);
    partialReceiver.init(receiveExpects);

    broadcast = new MPIDataFlowBroadcast(channel, middleTask, destinations, finalReceiver);
    broadcast.init(config, type, instancePlan, broadCastEdge);
    Map<Integer, List<Integer>> broadCastExpects = broadcast.receiveExpectedTaskIds();
    finalReceiver.init(broadCastExpects);
  }

  @Override
  public boolean sendPartial(int source, Object message) {
    return reduce.sendPartial(source, message);
  }

  @Override
  public boolean send(int source, Object message) {
    return reduce.send(source, message);
  }

  @Override
  public boolean send(int source, Object message, int path) {
    throw new RuntimeException("Not-implemented");
  }

  @Override
  public boolean sendPartial(int source, Object message, int path) {
    throw new RuntimeException("Not-implemented");
  }

  @Override
  public void progress() {
    finalReceiver.progress();
    partialReceiver.progress();

    broadcast.progress();
    reduce.progress();
  }

  @Override
  public void close() {

  }

  private class ReduceFinalReceiver implements MessageReceiver {
    // lets keep track of the messages
    // for each task we need to keep track of incoming messages
    private Map<Integer, Map<Integer, List<Object>>> messages = new HashMap<>();
    private Map<Integer, Map<Integer, Integer>> counts = new HashMap<>();

    private int count = 0;

    @Override
    public void init(Map<Integer, List<Integer>> expectedIds) {
      for (Map.Entry<Integer, List<Integer>> e : expectedIds.entrySet()) {
        Map<Integer, List<Object>> messagesPerTask = new HashMap<>();
        Map<Integer, Integer> countsPerTask = new HashMap<>();

        for (int i : e.getValue()) {
          messagesPerTask.put(i, new ArrayList<Object>());
          countsPerTask.put(i, 0);
        }

        LOG.info(String.format("%d Final Task %d receives from %s",
            executor, e.getKey(), e.getValue().toString()));

        messages.put(e.getKey(), messagesPerTask);
        counts.put(e.getKey(), countsPerTask);
      }
    }

    @Override
    public boolean onMessage(int source, int path, int target, Object object) {
//      LOG.info(String.format("%d received message %d", executor, target));
      // add the object to the map
      boolean canAdd = true;
      try {
        List<Object> m = messages.get(target).get(source);
        Integer c = counts.get(target).get(source);
        if (m.size() > 128) {
          canAdd = false;
        } else {
          m.add(object);
          counts.get(target).put(source, c + 1);
        }

        return canAdd;
      } catch (Throwable t) {
        t.printStackTrace();
      }
      return true;
    }

    public void progress() {
      for (int t : messages.keySet()) {
        boolean canProgress = true;
        while (canProgress) {
          // now check weather we have the messages for this source
          Map<Integer, List<Object>> map = messages.get(t);
          Map<Integer, Integer> cMap = counts.get(t);
          boolean found = true;
          Object o = null;
          for (Map.Entry<Integer, List<Object>> e : map.entrySet()) {
            if (e.getValue().size() == 0) {
              found = false;
              canProgress = false;
            } else {
              o = e.getValue().get(0);
            }
          }
          if (found) {
            if (broadcast.send(t, o)) {
              count++;
              for (Map.Entry<Integer, List<Object>> e : map.entrySet()) {
                o = e.getValue().remove(0);
              }
              for (Map.Entry<Integer, Integer> e : cMap.entrySet()) {
                Integer i = e.getValue();
                cMap.put(e.getKey(), i - 1);
              }
//                  LOG.info(String.format("%d reduce send true", id));
            } else {
              canProgress = false;
//                  LOG.info(String.format("%d reduce send false", id));
            }
          }
        }
      }
    }
  }
}
