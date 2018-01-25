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
package edu.iu.dsc.tws.examples.basic.batch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.core.TWSCommunication;
import edu.iu.dsc.tws.comms.core.TWSNetwork;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.examples.IntData;
import edu.iu.dsc.tws.examples.Utils;
import edu.iu.dsc.tws.rsched.spi.container.IContainer;
import edu.iu.dsc.tws.rsched.spi.resource.ResourcePlan;

public class BaseBatchAggregate implements IContainer {
  private static final Logger LOG = Logger.getLogger(BaseBatchAggregate.class.getName());

  private DataFlowOperation aggregate;

  private ResourcePlan resourcePlan;

  private int id;

  private Config config;

  private static final int NO_OF_TASKS = 8;

  private int noOfTasksPerExecutor = 2;

  @Override
  public void init(Config cfg, int containerId, ResourcePlan plan) {
    LOG.log(Level.INFO, "Starting the example with container id: " + plan.getThisId());

    this.config = cfg;
    this.resourcePlan = plan;
    this.id = containerId;
    this.noOfTasksPerExecutor = NO_OF_TASKS / plan.noOfContainers();

    // lets create the task plan
    TaskPlan taskPlan = Utils.createReduceTaskPlan(cfg, plan, NO_OF_TASKS);
    //first get the communication config file
    TWSNetwork network = new TWSNetwork(cfg, taskPlan);

    TWSCommunication channel = network.getDataFlowTWSCommunication();

    Set<Integer> sources = new HashSet<>();
    for (int i = 0; i < NO_OF_TASKS; i++) {
      sources.add(i);
    }
    int dest = NO_OF_TASKS;

    Map<String, Object> newCfg = new HashMap<>();

    LOG.info("Setting up reduce dataflow operation");
    try {
      aggregate = channel.gather(newCfg, MessageType.OBJECT, 0, sources,
          dest, new FinalReduceReceive());

      for (int i = 0; i < noOfTasksPerExecutor; i++) {
        // the map thread where data is produced
        LOG.info(String.format("%d Starting thread %d", id, i + id * noOfTasksPerExecutor));
        Thread mapThread = new Thread(new MapWorker(i + id * noOfTasksPerExecutor));
        mapThread.start();
      }
      // we need to progress the communication
      while (true) {
        try {
          // progress the channel
          channel.progress();
          // we should progress the communication directive
          aggregate.progress();
          Thread.yield();
        } catch (Throwable t) {
          t.printStackTrace();
        }
      }
    } catch (Throwable t) {
      t.printStackTrace();
    }
  }

  /**
   * We are running the map in a separate thread
   */
  private class MapWorker implements Runnable {
    private int task = 0;
    private int sendCount = 0;
    MapWorker(int task) {
      this.task = task;
    }

    @Override
    public void run() {
      try {
        LOG.log(Level.INFO, "Starting map worker: " + id);
//      MPIBuffer data = new MPIBuffer(1024);
        IntData data = generateData();
        for (int i = 0; i < 11000; i++) {
          // lets generate a message
          while (!aggregate.send(task, data, 0)) {
            // lets wait a litte and try again
            try {
              Thread.sleep(1);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }
//          LOG.info(String.format("%d sending to %d", id, task)
//              + " count: " + sendCount++);
          if (i % 1000 == 0) {
            LOG.info(String.format("%d sent %d", id, i));
          }
          Thread.yield();
        }
        LOG.info(String.format("%d Done sending", id));
      } catch (Throwable t) {
        t.printStackTrace();
      }
    }
  }

  private class FinalReduceReceive implements MessageReceiver {
    // lets keep track of the messages
    // for each task we need to keep track of incoming messages
    private Map<Integer, Map<Integer, List<Object>>> messages = new HashMap<>();
    private Map<Integer, Map<Integer, Integer>> counts = new HashMap<>();

    private int count = 0;

    private long start = System.nanoTime();

    @Override
    public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
      for (Map.Entry<Integer, List<Integer>> e : expectedIds.entrySet()) {
        Map<Integer, List<Object>> messagesPerTask = new HashMap<>();
        Map<Integer, Integer> countsPerTask = new HashMap<>();

        for (int i : e.getValue()) {
          messagesPerTask.put(i, new ArrayList<Object>());
          countsPerTask.put(i, 0);
        }

        LOG.info(String.format("%d Final Task %d receives from %s",
            id, e.getKey(), e.getValue().toString()));

        messages.put(e.getKey(), messagesPerTask);
        counts.put(e.getKey(), countsPerTask);
      }
    }

    @Override
    public boolean onMessage(int source, int path, int target, int flags, Object object) {
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
            if (o != null) {
              count++;
              if (count % 1000 == 0) {
                LOG.info(String.format("%d Last %d count: %d %s",
                    id, t, count, counts));
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

  /**
   * Generate data with an integer array
   *
   * @return IntData
   */
  private IntData generateData() {
    int s = 64000;
    int[] d = new int[s];
    for (int i = 0; i < s; i++) {
      d[i] = i;
    }
    return new IntData(d);
  }
}
