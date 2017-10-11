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
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.Message;
import edu.iu.dsc.tws.comms.api.Operation;
import edu.iu.dsc.tws.comms.core.TWSCommunication;
import edu.iu.dsc.tws.comms.core.TWSNetwork;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.mpi.io.DefaultMessageReceiver;
import edu.iu.dsc.tws.comms.mpi.io.MPIMessageDeSerializer;
import edu.iu.dsc.tws.comms.mpi.io.MPIMessageSerializer;
import edu.iu.dsc.tws.rsched.spi.container.IContainer;
import edu.iu.dsc.tws.rsched.spi.resource.ResourcePlan;

public class BaseLoadBalanceCommunication implements IContainer {
  private static final Logger LOG = Logger.getLogger(BaseLoadBalanceCommunication.class.getName());

  private DataFlowOperation loadBalance;

  private ResourcePlan resourcePlan;

  private int id;

  private Config config;

  private BlockingQueue<Message> loadReceiveQueue = new ArrayBlockingQueue<Message>(1024);

  private enum Status {
    INIT,
    MAP_FINISHED,
    LOAD_RECEIVE_FINISHED,
  }

  private Status status;

  @Override
  public void init(Config cfg, int containerId, ResourcePlan plan) {
    this.config = cfg;
    this.resourcePlan = plan;
    this.id = containerId;
    this.status = Status.INIT;

    // lets create the task plan
    TaskPlan taskPlan = createTaskPlan(cfg, plan);
    //first get the communication config file
    TWSNetwork network = new TWSNetwork(cfg, taskPlan);

    TWSCommunication channel = network.getDataFlowTWSCommunication();

    Set<Integer> sources = new HashSet<>();
    Set<Integer> dests = new HashSet<>();
    Map<String, Object> newCfg = new HashMap<>();

    // this method calls the init method
    // I think this is wrong
    loadBalance = channel.setUpDataFlowOperation(Operation.REDUCE, id, sources,
        dests, newCfg, 0, new DefaultMessageReceiver(loadReceiveQueue),
        new MPIMessageDeSerializer(), new MPIMessageSerializer(),
        new DefaultMessageReceiver(loadReceiveQueue));

    // the map thread where data is produced
    Thread mapThread = new Thread(new MapWorker());

    mapThread.start();

    // now lets create the load balance receiving worker
    Thread loadBalanceWorker = new Thread(new LoadBalanceWorker());
    loadBalanceWorker.start();

    // we need to progress the communication
    while (status != Status.LOAD_RECEIVE_FINISHED) {
      channel.progress();
    }

    try {
      mapThread.join();
    } catch (InterruptedException e) {
      throw new RuntimeException("Failed to wait on threads");
    }
  }

  /**
   * We are running the map in a separate thread
   */
  private class MapWorker implements Runnable {
    @Override
    public void run() {
      for (int i = 0; i < 100000; i++) {
        Message message = Message.newBuilder().setPayload(generateData()).build();
        // lets generate a message
        loadBalance.sendCompleteMessage(message);
      }
    }
  }


  private class LoadBalanceWorker implements Runnable {
    @Override
    public void run() {
      while (true) {
        try {
          Message m = loadReceiveQueue.take();
          System.out.println("Received message");
        } catch (InterruptedException ignore) {
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
    int[] d = new int[10];
    for (int i = 0; i < 10; i++) {
      d[i] = i;
    }
    return new IntData(d);
  }

  /**
   * Let assume we have 1 task per container
   * @param plan the resource plan from scheduler
   * @return task plan
   */
  private TaskPlan createTaskPlan(Config cfg, ResourcePlan plan) {
    int noOfProcs = resourcePlan.noOfContainers();

    Map<Integer, Set<Integer>> executorToChannels = null;
    Map<Integer, Set<Integer>> groupsToChannels = null;
    int thisExecutor = 0;
    int thisTaskk = 0;

    TaskPlan taskPlan = new TaskPlan(executorToChannels, groupsToChannels, thisExecutor, thisTaskk);
    return taskPlan;
  }
}
