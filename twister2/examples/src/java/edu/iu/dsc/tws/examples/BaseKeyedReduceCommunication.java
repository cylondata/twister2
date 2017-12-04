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
package edu.iu.dsc.tws.examples;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.core.TWSCommunication;
import edu.iu.dsc.tws.comms.core.TWSNetwork;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.mpi.MPIBuffer;
import edu.iu.dsc.tws.comms.mpi.MPIContext;
import edu.iu.dsc.tws.rsched.spi.container.IContainer;
import edu.iu.dsc.tws.rsched.spi.resource.ResourcePlan;

public class BaseKeyedReduceCommunication implements IContainer {
  private static final Logger LOG = Logger.getLogger(BaseKeyedReduceCommunication.class.getName());

  private DataFlowOperation reduce;

  private ResourcePlan resourcePlan;

  private int id;

  private Config config;

  private static final int NO_OF_TASKS = 16;

  private int noOfTasksPerExecutor = 2;

  private enum Status {
    INIT,
    MAP_FINISHED,
    LOAD_RECEIVE_FINISHED,
  }

  private Status status;

  @Override
  public void init(Config cfg, int containerId, ResourcePlan plan) {
    LOG.log(Level.INFO, "Starting the example with container id: " + plan.getThisId());

    this.config = cfg;
    this.resourcePlan = plan;
    this.id = containerId;
    this.status = Status.INIT;
    this.noOfTasksPerExecutor = NO_OF_TASKS / plan.noOfContainers();

    // lets create the task plan
    TaskPlan taskPlan = Utils.createReduceTaskPlan(cfg, plan, NO_OF_TASKS);
    //first get the communication config file
    TWSNetwork network = new TWSNetwork(cfg, taskPlan);

    TWSCommunication channel = network.getDataFlowTWSCommunication();

    Set<Integer> sources = new HashSet<>();
    for (int i = 0; i < NO_OF_TASKS / 2; i++) {
      sources.add(i);
    }
    Set<Integer> destinations = new HashSet<>();
    for (int i = 0; i < NO_OF_TASKS / 2; i++) {
      destinations.add(NO_OF_TASKS / 2 + i);
    }

    Map<String, Object> newCfg = new HashMap<>();

    LOG.info("Setting up reduce dataflow operation");
    // this method calls the init method
    // I think this is wrong
    reduce = channel.keyedReduce(newCfg, MessageType.BUFFER, 0, sources,
        destinations, new FinalReduceReceive(), new PartialReduceWorker());

    if (id == 0 || id == 1) {
      // the map thread where data is produced
      Thread mapThread = new Thread(new MapWorker());
      mapThread.start();
    }

    // we need to progress the communication
    while (true) {
      try {
        // progress the channel
        channel.progress();
        // we should progress the communication directive
        reduce.progress();
        Thread.yield();
      } catch (Throwable t) {
        LOG.severe("Error occurred: " + id);
        t.printStackTrace();
      }
    }
  }

  /**
   * We are running the map in a separate thread
   */
  private class MapWorker implements Runnable {
    private int sendCount = 0;
    @Override
    public void run() {
      LOG.log(Level.INFO, "Starting map worker");
      MPIBuffer data = new MPIBuffer(1024);
      data.setSize(24);
      for (int i = 0; i < 5000; i++) {
        for (int j = 0; j < noOfTasksPerExecutor; j++) {
          // lets generate a message
          while (!reduce.send(j + id * noOfTasksPerExecutor, data, 0)) {
            // lets wait a litte and try again
            try {
              Thread.sleep(1);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }
          LOG.info(String.format("%d sending to %d", id, j + id * noOfTasksPerExecutor)
              + " count: " + sendCount++);
        }
        sendCount++;
        Thread.yield();
      }
      status = Status.MAP_FINISHED;
    }
  }

  /**
   * Reduce class will work on the reduce messages.
   */
  private class PartialReduceWorker implements MessageReceiver {

    private Queue<Object> pendingSends = new LinkedBlockingQueue<Object>();
    // lets keep track of the messages
    // for each task we need to keep track of incoming messages
    private Map<Integer, Map<Integer, List<Object>>> messages = new HashMap<>();

    /**
     * For each task in this exector, we will receive from the list of tasks in the given path
     *
     * @param expectedIds expected task ids
     */
    @Override
    public void init(Map<Integer, Map<Integer, List<Integer>>> expectedIds) {
      for (Map.Entry<Integer, Map<Integer, List<Integer>>> e : expectedIds.entrySet()) {
        Map<Integer, List<Object>> messagesPerTask = new HashMap<>();

        Map<Integer, List<Integer>> value = e.getValue();
        if (value.containsKey(MPIContext.DEFAULT_PATH)) {
          for (int i : value.get(MPIContext.DEFAULT_PATH)) {
            messagesPerTask.put(i, new ArrayList<Object>());
          }
        }
//        LOG.info(String.format("%d Partial Task %d receives from %s",
//              id, e.getKey(), e.getValue().get(MPIContext.DEFAULT_PATH).toString()));

        messages.put(e.getKey(), messagesPerTask);
      }
    }

    @Override
    public void onMessage(int source, int path, int target, Object object) {
      LOG.info(String.format("%d Message received for partial %d from %d", id, target, source));
      while (pendingSends.size() > 0) {
        boolean r = reduce.sendPartial(target, pendingSends.poll());
        if (!r) {
          break;
        }
      }
      // add the object to the map
      try {
        List<Object> m = messages.get(target).get(source);
        m.add(object);
        // now check weather we have the messages for this source
        Map<Integer, List<Object>> map = messages.get(target);
        boolean found = true;
        for (Map.Entry<Integer, List<Object>> e : map.entrySet()) {
          if (e.getValue().size() == 0) {
            found = false;
          }
        }
        if (found) {
          Object o = null;
          for (Map.Entry<Integer, List<Object>> e : map.entrySet()) {
            o = e.getValue().remove(0);
          }
          if (o != null) {
            if (pendingSends.size() > 0) {
              pendingSends.offer(object);
            } else {
              boolean inject = reduce.sendPartial(target, o);
              if (!inject) {
                pendingSends.offer(object);
              } /*else {
//                LOG.info(String.format("%d Inject partial %d count: %d", id, target, count++));
              }*/
            }
          } else {
            LOG.severe("We cannot find an object and this is not correct");
          }
        }
      } catch (Throwable t) {
        t.printStackTrace();
      }
    }
  }

  private class FinalReduceReceive implements MessageReceiver {
    // lets keep track of the messages
    // for each task we need to keep track of incoming messages
    private Map<Integer, Map<Integer, List<Object>>> messages = new HashMap<>();

    private int count = 0;

    private long start = System.nanoTime();

    @Override
    public void init(Map<Integer, Map<Integer, List<Integer>>> expectedIds) {
      for (Map.Entry<Integer, Map<Integer, List<Integer>>> e : expectedIds.entrySet()) {
        Map<Integer, List<Object>> messagesPerTask = new HashMap<>();

        Map<Integer, List<Integer>> value = e.getValue();
        if (value.containsKey(MPIContext.DEFAULT_PATH)) {
          for (int i : value.get(MPIContext.DEFAULT_PATH)) {
            messagesPerTask.put(i, new ArrayList<Object>());
          }
        }

//        LOG.info(String.format("%d Final Task %d receives from %s",
//            id, e.getKey(), e.getValue().get(MPIContext.DEFAULT_PATH).toString()));

        messages.put(e.getKey(), messagesPerTask);
      }
    }

    @Override
    public void onMessage(int source, int path, int target, Object object) {
      LOG.info(String.format("%d Message received for partial %d from %d", id, target, source));
      // add the object to the map
      if (count == 0) {
        start = System.nanoTime();
      }

      try {
        List<Object> m = messages.get(target).get(source);
        m.add(object);
        // now check weather we have the messages for this source
        Map<Integer, List<Object>> map = messages.get(target);
        boolean found = true;
        for (Map.Entry<Integer, List<Object>> e : map.entrySet()) {
          if (e.getValue().size() == 0) {
            found = false;
          }
        }
        if (found) {
          Object o = null;
          for (Map.Entry<Integer, List<Object>> e : map.entrySet()) {
            o = e.getValue().remove(0);
          }
          if (o != null) {
            count++;
            if (count % 1000 == 0) {
              LOG.info("Message received for last: " + source + " target: "
                  + target + " count: " + count);
            }
            if (count == 5000) {
              LOG.info("Total time: " + (System.nanoTime() - start) / 1000000);
            }
          } else {
            LOG.severe("We cannot find an object and this is not correct");
          }
        }
      } catch (Throwable t) {
        t.printStackTrace();
      }
    }
  }

  /**
   * Generate data with an integer array
   *
   * @return IntData
   */
  private IntData generateData() {
    int[] d = new int[10];
    for (int i = 0; i < 10; i++) {
      d[i] = i;
    }
    return new IntData(d);
  }
}
