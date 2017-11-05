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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageHeader;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.core.TWSCommunication;
import edu.iu.dsc.tws.comms.core.TWSNetwork;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.rsched.spi.container.IContainer;
import edu.iu.dsc.tws.rsched.spi.resource.ResourcePlan;

/**
 * This will be a map-reduce job only using the communication primitives
 */
public class BaseReduceCommunication implements IContainer {
  private static final Logger LOG = Logger.getLogger(BaseReduceCommunication.class.getName());

  private DataFlowOperation reduce;

  private ResourcePlan resourcePlan;

  private int id;

  private Config config;

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

    // lets create the task plan
    TaskPlan taskPlan = Utils.createReduceTaskPlan(cfg, plan, 8);
    //first get the communication config file
    TWSNetwork network = new TWSNetwork(cfg, taskPlan);

    TWSCommunication channel = network.getDataFlowTWSCommunication();

    Set<Integer> sources = new HashSet<>();
    for (int i = 0; i < 7; i++) {
      sources.add(i);
    }
    int dest = 7;

    Map<String, Object> newCfg = new HashMap<>();

    LOG.info("Setting up reduce dataflow operation");
    // this method calls the init method
    // I think this is wrong
    reduce = channel.reduce(newCfg, MessageType.OBJECT, 0, sources,
        dest, new FinalReduceReceive(), new PartialReduceWorker());

    if (containerId == 0) {
      // the map thread where data is produced
      Thread mapThread = new Thread(new MapWorker());
      mapThread.start();

      // we need to progress the communication
      while (true) {
        // progress the channel
        channel.progress();
        // we should progress the communication directive
        reduce.progress();
        Thread.yield();
      }
    } else if (containerId == 1) {
      while (status != Status.LOAD_RECEIVE_FINISHED) {
        channel.progress();
        reduce.progress();
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
      for (int i = 0; i < 100000; i++) {
        IntData data = generateData();
        // lets generate a message
        while (!reduce.send(0, data)) {
          // lets wait a litte and try again
          try {
            Thread.sleep(1);
          } catch (InterruptedException e) {
            e.printStackTrace();
          }
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
    @Override
    public void init(Map<Integer, List<Integer>> expectedIds) {
    }

    @Override
    public void onMessage(MessageHeader header, Object object) {
      reduce.injectPartialResult(0, object);
    }
  }

  private class FinalReduceReceive implements MessageReceiver {
    @Override
    public void init(Map<Integer, List<Integer>> expectedIds) {
    }

    @Override
    public void onMessage(MessageHeader header, Object object) {

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
