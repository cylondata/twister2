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
package edu.iu.dsc.tws.executor.comm;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MultiMessageReceiver;
import edu.iu.dsc.tws.comms.api.TWSChannel;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.dfw.DataFlowMultiReduce;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.executor.EdgeGenerator;
import edu.iu.dsc.tws.task.api.IMessage;

public class KeyedReduceOperation extends AbstractParallelOperation {
  private static final Logger LOG = Logger.getLogger(KeyedReduceOperation.class.getName());

  protected DataFlowMultiReduce op;

  public KeyedReduceOperation(Config config, TWSChannel network, TaskPlan tPlan) {
    super(config, network, tPlan);
  }

  public void prepare(Set<Integer> sources, Set<Integer> dests, EdgeGenerator e,
                      DataType dataType, String edgeName) {
    this.edge = e;
    op = new DataFlowMultiReduce(channel, sources, dests, new FinalReduceReceive(),
        new PartialReduceWorker(), dests);
    communicationEdge = e.generate(edgeName);
    op.init(config, Utils.dataTypeToMessageType(dataType), taskPlan, communicationEdge);
  }

  @Override
  public void send(int source, IMessage message) {
    LOG.info("Source : " + source + ", " + message.getContent());
    op.send(source, message.getContent(), 0);
  }

  @Override
  public void send(int source, IMessage message, int dest) {
    op.send(source, message.getContent(), 0, dest);
  }

  @Override
  public void progress() {
    op.progress();
  }

  /**
   * Reduce class will work on the reduce messages.
   */
  private class PartialReduceWorker implements MultiMessageReceiver {

    // lets keep track of the messages
    // for each task we need to keep track of incoming messages
    private Map<Integer, Map<Integer, List<Object>>> messages = new HashMap<>();
    private Map<Integer, Map<Integer, Integer>> counts = new HashMap<>();

    private int count = 0;

    private long start = System.nanoTime();

    @Override
    public void init(Config cfg, DataFlowOperation operation,
                     Map<Integer, Map<Integer, List<Integer>>> expectedIds) {
      Map<Integer, List<Integer>> exp = expectedIds.get(0);
      for (Map.Entry<Integer, List<Integer>> e : exp.entrySet()) {
        Map<Integer, List<Object>> messagesPerTask = new HashMap<>();
        Map<Integer, Integer> countsPerTask = new HashMap<>();

        for (int i : e.getValue()) {
          messagesPerTask.put(i, new ArrayList<Object>());
          countsPerTask.put(i, 0);
        }

        LOG.info(String.format("Partial Task %d receives from %s",
            e.getKey(), e.getValue().toString()));

        messages.put(e.getKey(), messagesPerTask);
        counts.put(e.getKey(), countsPerTask);
      }
    }

    @Override
    public boolean onMessage(int source, int path, int target, int flags, Object object) {
//      LOG.info(String.format("%d Message received for target %d source %d %d path",
//          id, target, source, path));
      // add the object to the map
      boolean canAdd = true;
      try {
        List<Object> m = messages.get(target).get(source);
        Integer c = counts.get(target).get(source);
        if (m.size() > 128) {
//          if (count % 1 == 0) {
//            LOG.info(String.format("%d Partial false %d %d %s", id, source, m.size(), counts));
//          }
          canAdd = false;
        } else {
//          if (count % 10 == 0) {
//          }
          m.add(object);
          counts.get(target).put(source, c + 1);
//          LOG.info(String.format("%d Partial true %d %d %s", id, source, m.size(), counts));
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
            if (o != null) {
              if (op.sendPartial(t, o, 0, 0)) {
                count++;
                for (Map.Entry<Integer, List<Object>> e : map.entrySet()) {
                  o = e.getValue().remove(0);
                }
                for (Map.Entry<Integer, Integer> e : cMap.entrySet()) {
                  Integer i = e.getValue();
                  cMap.put(e.getKey(), i - 1);
                }
//                LOG.info(String.format("%d reduce send true", id));
              } else {
                canProgress = false;
//                LOG.info(String.format("%d reduce send false", id));
              }
//              if (count % 100 == 0) {
//                LOG.info(String.format("%d Inject partial %d count: %d %s",
//                    id, t, count, counts));
//              }
            } else {
              canProgress = false;
              LOG.severe("We cannot find an object and this is not correct");
            }
          }
        }
      }
    }
  }

  private class FinalReduceReceive implements MultiMessageReceiver {
    // lets keep track of the messages
    // for each task we need to keep track of incoming messages
    private Map<Integer, Map<Integer, List<Object>>> messages = new HashMap<>();
    private Map<Integer, Map<Integer, Integer>> counts = new HashMap<>();

    private int count = 0;

    private long start = System.nanoTime();

    @Override
    public void init(Config cfg, DataFlowOperation operation,
                     Map<Integer, Map<Integer, List<Integer>>> exp) {
      Map<Integer, List<Integer>> expectedIds = exp.get(0);
      for (Map.Entry<Integer, List<Integer>> e : expectedIds.entrySet()) {
        Map<Integer, List<Object>> messagesPerTask = new HashMap<>();
        Map<Integer, Integer> countsPerTask = new HashMap<>();

        for (int i : e.getValue()) {
          messagesPerTask.put(i, new ArrayList<Object>());
          countsPerTask.put(i, 0);
        }

        messages.put(e.getKey(), messagesPerTask);
        counts.put(e.getKey(), countsPerTask);
      }
      LOG.info(String.format("Final Task receives from %s", expectedIds));
    }

    @Override
    public boolean onMessage(int source, int path, int target, int flags, Object object) {
//      LOG.info(String.format("%d Final receive source %d path %d target %d",
//          id, source, path, target));
      // add the object to the map
      boolean canAdd = true;
      if (count == 0) {
        start = System.nanoTime();
      }

      try {
        List<Object> m = messages.get(target).get(source);
        Integer c = counts.get(target).get(source);
        if (m.size() > 128) {
          canAdd = false;
//          LOG.info(String.format("%d Final false %d %d %s", id, source, m.size(), counts));
        } else {
          m.add(object);
          counts.get(target).put(source, c + 1);
//          LOG.info(String.format("%d Final true %d %d %s", id, source, m.size(), counts));
        }

        return canAdd;
      } catch (Throwable t) {
        t.printStackTrace();
      }
      return true;
    }

    public void progress() {
      for (int t : messages.keySet()) {
        Map<Integer, Integer> cMap = counts.get(t);
        boolean canProgress = true;
        while (canProgress) {
          // now check weather we have the messages for this source
          Map<Integer, List<Object>> map = messages.get(t);
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
            for (Map.Entry<Integer, List<Object>> e : map.entrySet()) {
              o = e.getValue().remove(0);
            }
            for (Map.Entry<Integer, Integer> e : cMap.entrySet()) {
              Integer i = e.getValue();
              cMap.put(e.getKey(), i - 1);
            }
            if (o != null) {
              count++;
              if (count % 100 == 0) {
                LOG.info(String.format("Last %d count: %d %s", t, count, counts));
              }
              if (count >= 10000) {
                LOG.info("Total time: " + (System.nanoTime() - start) / 1000000
                    + " Count: " + count);
              }
            } else {
              LOG.severe("We cannot find an object and this is not correct");
            }
          }
        }
      }
    }
  }


}
