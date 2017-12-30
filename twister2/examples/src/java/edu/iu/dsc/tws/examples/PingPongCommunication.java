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
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.core.TWSCommunication;
import edu.iu.dsc.tws.comms.core.TWSNetwork;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.rsched.spi.container.IContainer;
import edu.iu.dsc.tws.rsched.spi.resource.ResourcePlan;

public class PingPongCommunication implements IContainer {
  private static final Logger LOG = Logger.getLogger(PingPongCommunication.class.getName());

  private DataFlowOperation direct;

  private enum Status {
    INIT,
    MAP_FINISHED,
    LOAD_RECEIVE_FINISHED,
  }

  private Status status;

  /**
   * Initialize the container
   * @param cfg
   * @param containerId
   * @param plan
   */
  public void init(Config cfg, int containerId, ResourcePlan plan) {
    LOG.log(Level.INFO, "Starting the example with container id: " + plan.getThisId());

    this.status = Status.INIT;

    // lets create the task plan
    TaskPlan taskPlan = Utils.createTaskPlan(cfg, plan);
    //first get the communication config file
    TWSNetwork network = new TWSNetwork(cfg, taskPlan);

    TWSCommunication channel = network.getDataFlowTWSCommunication();

    // we are sending messages from 0th task to 1st task
    Set<Integer> sources = new HashSet<>();
    sources.add(0);
    int dests = 1;
    Map<String, Object> newCfg = new HashMap<>();

    LOG.info("Setting up reduce dataflow operation");
    // this method calls the init method
    // I think this is wrong
    direct = channel.direct(newCfg, MessageType.OBJECT, 0, sources,
        dests, new PingPongReceive());

    if (containerId == 0) {
      // the map thread where data is produced
      Thread mapThread = new Thread(new MapWorker());

      LOG.log(Level.INFO, "Starting map thread");
      mapThread.start();

      // we need to progress the communication
      while (true) {
        // progress the channel
        channel.progress();
        // we should progress the communication directive
        direct.progress();
        Thread.yield();
      }
    } else if (containerId == 1) {
      while (status != Status.LOAD_RECEIVE_FINISHED) {
        channel.progress();
        direct.progress();
      }
    }
  }

  private class PingPongReceive implements MessageReceiver {
    private int count = 0;
    @Override
    public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    }

    @Override
    public boolean onMessage(int source, int path, int target, int flags, Object object) {
      count++;
      if (count % 10000 == 0) {
        LOG.info("received message: " + count);
      }
      if (count == 100000) {
        status = Status.LOAD_RECEIVE_FINISHED;
      }
      return true;
    }

    @Override
    public void progress() {

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
        while (!direct.send(0, data, 0)) {
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
