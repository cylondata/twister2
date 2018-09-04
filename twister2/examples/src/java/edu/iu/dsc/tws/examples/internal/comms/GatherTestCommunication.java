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
package edu.iu.dsc.tws.examples.internal.comms;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.net.Network;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.discovery.IWorkerController;
import edu.iu.dsc.tws.common.resource.AllocatedResources;
import edu.iu.dsc.tws.common.worker.IPersistentVolume;
import edu.iu.dsc.tws.common.worker.IVolatileVolume;
import edu.iu.dsc.tws.common.worker.IWorker;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.api.TWSChannel;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.dfw.DataFlowGather;
import edu.iu.dsc.tws.examples.Utils;

public class GatherTestCommunication implements IWorker {
  private static final Logger LOG = Logger.
      getLogger(GatherTestCommunication.class.getName());

  private DataFlowGather aggregate;

  private int id;

  private static final int NO_OF_TASKS = 8;

  private long startTime = 0;

  @Override
  public void execute(Config cfg, int workerID, AllocatedResources resources,
                      IWorkerController workerController,
                      IPersistentVolume persistentVolume,
                      IVolatileVolume volatileVolume) {
    LOG.log(Level.INFO, "Starting the example with container id: " + resources.getWorkerId());

    this.id = workerID;
    int noOfTasksPerExecutor = NO_OF_TASKS / resources.getNumberOfWorkers();

    // lets create the task plan
    TaskPlan taskPlan = Utils.createReduceTaskPlan(cfg, resources, NO_OF_TASKS);
    //first get the communication config file
    TWSChannel network = Network.initializeChannel(cfg, workerController, resources);

    Set<Integer> sources = new HashSet<>();
    for (int i = 0; i < NO_OF_TASKS; i++) {
      sources.add(i);
    }
    int dest = NO_OF_TASKS;

    // this method calls the execute method
    // I think this is wrong
    aggregate = new DataFlowGather(network, sources,
        dest, new FinalGatherReceiver(), 0, 0, cfg, MessageType.OBJECT, taskPlan, 0);
    aggregate.init(cfg, MessageType.OBJECT, taskPlan, 0);

    for (int i = 0; i < noOfTasksPerExecutor; i++) {
      // the map thread where data is produced
      LOG.info(String.format("%d Starting %d", id, i + id * noOfTasksPerExecutor));
      Thread mapThread = new Thread(new MapWorker(i + id * noOfTasksPerExecutor));
      mapThread.start();
    }
    // we need to communicationProgress the communication
    while (true) {
      try {
        // communicationProgress the channel
        network.progress();
        // we should communicationProgress the communication directive
        aggregate.progress();
        Thread.yield();
      } catch (Throwable t) {
        t.printStackTrace();
      }
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
        startTime = System.nanoTime();
        for (int i = 0; i < 1; i++) {
          int[] data = {task, task * 100};
          // lets generate a message
//          KeyedContent mesage = new KeyedContent(task, data,
//              MessageType.INTEGER, MessageType.OBJECT);
//
          while (!aggregate.send(task, data, 0)) {
            // lets wait a litte and try again
            try {
              Thread.sleep(1);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }
          Thread.yield();
        }
        LOG.info(String.format("%d Done sending", id));
      } catch (Throwable t) {
        t.printStackTrace();
      }
    }
  }

  private class FinalGatherReceiver implements MessageReceiver {
    // lets keep track of the messages
    // for each task we need to keep track of incoming messages
    private List<Integer> dataList;

    private int count = 0;

    private long start = System.nanoTime();

    @Override
    public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
      dataList = new ArrayList<Integer>();
    }

    @Override
    @SuppressWarnings("unchecked")
    public boolean onMessage(int source, int path, int target, int flags, Object object) {

      // add the object to the map
      boolean canAdd = true;
      if (count == 0) {
        start = System.nanoTime();
      }
      if (object instanceof List) {
        List<Object> datalist = (List<Object>) object;
        for (Object o : datalist) {
          int[] data = (int[]) o;
          dataList.add(data[0]);
        }
      } else {
        int[] data = (int[]) object;
        dataList.add(data[0]);
      }
      LOG.info("Gather results (only the first int of each array)"
          + Arrays.toString(dataList.toArray()));

      return true;
    }

    public boolean progress() {
      return true;
    }
  }
}

