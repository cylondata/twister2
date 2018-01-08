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
package edu.iu.dsc.tws.examples.basic.comms;

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
import edu.iu.dsc.tws.comms.mpi.MPIBuffer;
import edu.iu.dsc.tws.examples.IntData;
import edu.iu.dsc.tws.examples.Utils;
import edu.iu.dsc.tws.rsched.spi.container.IContainer;
import edu.iu.dsc.tws.rsched.spi.resource.ResourcePlan;

public class BaseLoadBalanceCommunication implements IContainer {
  private static final Logger LOG = Logger.getLogger(BaseLoadBalanceCommunication.class.getName());

  private DataFlowOperation loadBalance;

  private ResourcePlan resourcePlan;

  private int id;

  private Config config;

  private static final int NO_OF_TASKS = 8;

  private int noOfTasksPerExecutor = 2;

  private enum Status {
    INIT,
    MAP_FINISHED,
    LOAD_RECEIVE_FINISHED,
  }

  private Status status;

  private TWSCommunication channel;

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

    channel = network.getDataFlowTWSCommunication();

    Set<Integer> sources = new HashSet<>();
    Set<Integer> dests = new HashSet<>();
    for (int i = 0; i < NO_OF_TASKS; i++) {
      if (i < NO_OF_TASKS / 2) {
        sources.add(i);
      } else {
        dests.add(i);
      }
    }
    LOG.info(String.format("Loadbalance: sources %s destinations: %s", sources, dests));

    Map<String, Object> newCfg = new HashMap<>();

    LOG.info("Setting up reduce dataflow operation");
    // this method calls the init method
    // I think this is wrong
    loadBalance = channel.loadBalance(newCfg, MessageType.BUFFER, 0,
        sources, dests, new LoadBalanceReceiver());
    // the map thread where data is produced
    LOG.info("Starting worker: " + id);

    // we need to progress the communication
    try {
      if (id == 0 || id == 1) {
        MPIBuffer data = new MPIBuffer(1024);
        data.setSize(24);
        for (int i = 0; i < 50000; i++) {
          mapFunction(data);
          channel.progress();
          // we should progress the communication directive
          loadBalance.progress();
        }
        while (true) {
          channel.progress();
          // we should progress the communication directive
          loadBalance.progress();
        }
      } else {
        while (true) {
          channel.progress();
          // we should progress the communication directive
          loadBalance.progress();
        }
      }
    } catch (Throwable t) {
      t.printStackTrace();
    }
  }

  private void mapFunction(Object data) {
    for (int j = 0; j < NO_OF_TASKS / 4; j++) {
      while (!loadBalance.send(id * 2 + j, data, 0)) {
        // lets wait a litte and try again
        channel.progress();
        // we should progress the communication directive
        loadBalance.progress();
      }
    }
    status = Status.MAP_FINISHED;
  }

  private class LoadBalanceReceiver implements MessageReceiver {
    private int count = 0;
    private long start = System.nanoTime();
    @Override
    public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
      for (Map.Entry<Integer, List<Integer>> e : expectedIds.entrySet()) {
        LOG.info(String.format("%d Final Task %d receives from %s",
            id, e.getKey(), e.getValue().toString()));
      }
    }

    @Override
    public boolean onMessage(int source, int path, int target, int flags, Object object) {
      if (count == 0) {
        start = System.nanoTime();
      }
      count++;
      if (count % 5000 == 0) {
        LOG.info(id + " Total time: " + (System.nanoTime() - start) / 1000000 + " " + count);
      }
      if (count > 100000) {
        LOG.info("More than");
      }
      return true;
    }

    @Override
    public void progress() {
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
