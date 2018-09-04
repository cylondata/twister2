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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Submitter;
import edu.iu.dsc.tws.api.job.Twister2Job;
import edu.iu.dsc.tws.api.net.Network;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.discovery.IWorkerController;
import edu.iu.dsc.tws.common.resource.AllocatedResources;
import edu.iu.dsc.tws.common.resource.WorkerComputeResource;
import edu.iu.dsc.tws.common.worker.IPersistentVolume;
import edu.iu.dsc.tws.common.worker.IVolatileVolume;
import edu.iu.dsc.tws.common.worker.IWorker;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.api.TWSChannel;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.dfw.DataFlowBroadcast;
import edu.iu.dsc.tws.examples.IntData;
import edu.iu.dsc.tws.examples.Utils;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;

public class BroadcastCommunication implements IWorker {
  private static final Logger LOG = Logger.getLogger(BroadcastCommunication.class.getName());

  private DataFlowBroadcast broadcast;

  private AllocatedResources resourcePlan;

  private int id;

  private Config config;

  private static final int NO_OF_TASKS = 32;

  private int noOfTasksPerExecutor = 2;

  private enum Status {
    INIT,
    MAP_FINISHED,
    LOAD_RECEIVE_FINISHED,
  }

  private Status status;

  @Override
  public void execute(Config cfg,
                      int workerID,
                      AllocatedResources resources,
                      IWorkerController workerController,
                      IPersistentVolume persistentVolume,
                      IVolatileVolume volatileVolume) {

    LOG.log(Level.INFO, "Starting the example with container id: " + resources.getWorkerId());
    this.config = cfg;
    this.resourcePlan = resources;
    this.id = workerID;
    this.status = Status.INIT;
    this.noOfTasksPerExecutor = NO_OF_TASKS / resources.getNumberOfWorkers();

    // lets create the task plan
    TaskPlan taskPlan = Utils.createReduceTaskPlan(cfg, resources, NO_OF_TASKS);
    LOG.log(Level.INFO, "Task plan: " + taskPlan);
    //first get the communication config file
    TWSChannel network = Network.initializeChannel(cfg, workerController, resources);

    Set<Integer> sources = new HashSet<>();
    for (int i = 0; i < NO_OF_TASKS; i++) {
      sources.add(i);
    }
    int dest = NO_OF_TASKS;

    Map<String, Object> newCfg = new HashMap<>();

    LOG.info(String.format("Setting up broadcast dataflow operation %d %s", dest, sources));
    // this method calls the execute method
    // I think this is wrong
    broadcast = new DataFlowBroadcast(network, dest, sources, new BCastReceive());
    broadcast.init(config, MessageType.OBJECT, taskPlan, 0);

    // the map thread where data is produced
    if (id == 0) {
      Thread mapThread = new Thread(new MapWorker());
      mapThread.start();
    }

    // we need to communicationProgress the communication
    while (true) {
      try {
        // communicationProgress the channel
        network.progress();
        // we should communicationProgress the communication directive
        broadcast.progress();
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
    private int sendCount = 0;
    @Override
    public void run() {
      LOG.log(Level.INFO, "Starting map worker");
      for (int i = 0; i < 1000; i++) {
        IntData data = generateData();
        data.setId(i);
        // lets generate a message
        while (!broadcast.send(NO_OF_TASKS, data, 0)) {
          // lets wait a litte and try again
          broadcast.progress();
        }
        sendCount++;
        Thread.yield();
      }
      status = Status.MAP_FINISHED;
    }
  }

  private class BCastReceive implements MessageReceiver {
    private int count = 0;
    private Map<Integer, List<Integer>> receiveIds = new HashMap<>();
    public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
      for (Map.Entry<Integer, List<Integer>> e : expectedIds.entrySet()) {
        LOG.info(String.format("%d Final Task %d receives from %s",
            id, e.getKey(), e.getValue().toString()));
        receiveIds.put(e.getKey(), new ArrayList<>());
      }
    }

    @Override
    public boolean onMessage(int source, int path, int target, int flags, Object object) {
      try {
        count++;
        if (count % 1 == 0) {
          LOG.info("Message received for last: " + source + " target: "
              + target + " count: " + count);
        }
        IntData data = (IntData) object;
        int sequence = data.getId();

        List<Integer> list = receiveIds.get(target);
        list.add(sequence);

        Set<Integer> r = new HashSet<>(list);
        if (list.size() != r.size()) {
          LOG.log(Level.INFO, "Duplicates", new RuntimeException("Dups"));
        }
      } catch (NullPointerException e) {
        LOG.log(Level.INFO, "Message received for last: " + source + " target: "
            + target + " count: " + count + " rids: " + receiveIds, e);
      }

      return true;
    }

    @Override
    public boolean progress() {
      return true;
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

  public static void main(String[] args) {
    // first load the configurations from command line and config files
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    // build JobConfig
    HashMap<String, Object> configurations = new HashMap<>();
    configurations.put(SchedulerContext.THREADS_PER_WORKER, 8);

    // build JobConfig
    JobConfig jobConfig = new JobConfig();
    jobConfig.putAll(configurations);

    // build the job
    Twister2Job twister2Job = Twister2Job.newBuilder()
        .setName("basic-broadcast")
        .setWorkerClass(BroadcastCommunication.class.getName())
        .setRequestResource(new WorkerComputeResource(2, 1024), 4)
        .setConfig(jobConfig)
        .build();

    // now submit the job
    Twister2Submitter.submitJob(twister2Job, config);
  }
}
