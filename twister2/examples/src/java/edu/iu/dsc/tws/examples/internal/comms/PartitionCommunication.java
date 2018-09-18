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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
import edu.iu.dsc.tws.comms.api.MessageFlags;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.api.TWSChannel;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.dfw.DataFlowPartition;
import edu.iu.dsc.tws.comms.dfw.io.partition.PartitionPartialReceiver;
import edu.iu.dsc.tws.examples.IntData;
import edu.iu.dsc.tws.examples.Utils;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;

/**
 * This will be a map-partition job only using the communication primitives
 */
public class PartitionCommunication implements IWorker {
  private static final Logger LOG = Logger.getLogger(PartitionCommunication.class.getName());

  private DataFlowPartition partition;

  private int id;

  private static final int NO_OF_TASKS = 4;

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
    Set<Integer> dests = new HashSet<>();
    for (int i = 0; i < NO_OF_TASKS; i++) {
      sources.add(i);
      dests.add(i);
    }

    LOG.info("Setting up partition dataflow operation");
    try {
      // this method calls the execute method
      // I think this is wrong
      Map<Integer, List<Integer>> expectedIds = new HashMap<>();
      for (int i = 0; i < NO_OF_TASKS; i++) {
        expectedIds.put(i, new ArrayList<>());
        for (int j = 0; j < NO_OF_TASKS; j++) {
          if (!(i == j)) {
            expectedIds.get(i).add(j);

          }
        }
      }
      partition = new DataFlowPartition(network,
          sources, dests, new FinalPartitionReciver(), new PartitionPartialReceiver(),
          DataFlowPartition.PartitionStratergy.DIRECT);
      partition.init(cfg, MessageType.BYTE, taskPlan, 0);

      // partition.setMemoryMapped(true);

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
          partition.progress();
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
//        int[] data = {task, task * 100};
        for (int i = 0; i < NO_OF_TASKS; i++) {
//          if (i == task) {
//            continue;
//          }
          byte[] data = new byte[12];
          data[0] = 'a';
          data[1] = 'b';
          data[2] = 'c';
          data[3] = 'd';
          data[4] = 'd';
          data[5] = 'd';
          data[6] = 'd';
          data[7] = 'd';
          int flags = MessageFlags.LAST;
          while (!partition.send(task, data, flags, i)) {
            // lets wait a litte and try again
            try {
              Thread.sleep(1);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }
        }
        LOG.info(String.format("%d Done sending", id));
      } catch (Throwable t) {
        t.printStackTrace();
      }
    }
  }

  private class FinalPartitionReciver implements MessageReceiver {
    private Map<Integer, Map<Integer, Boolean>> finished;

    private long start = System.nanoTime();

    @Override
    public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
      finished = new ConcurrentHashMap<>();
      for (Integer integer : expectedIds.keySet()) {
        Map<Integer, Boolean> perTarget = new ConcurrentHashMap<>();
        for (Integer integer1 : expectedIds.get(integer)) {
          perTarget.put(integer1, false);
        }
        finished.put(integer, perTarget);
      }
    }

    @Override
    public boolean onMessage(int source, int path, int target, int flags, Object object) {
      // add the object to the map
      if ((flags & MessageFlags.LAST) == MessageFlags.LAST) {
        finished.get(target).put(source, true);
      }

      if (((flags & MessageFlags.LAST) == MessageFlags.LAST) && isAllFinished(target)) {
        System.out.println(Arrays.toString((byte[]) object));
        System.out.printf("All Done for Task %d \n", target);
      }
      return true;
    }

    private boolean isAllFinished(int target) {
      boolean isDone = true;
      for (Boolean bol : finished.get(target).values()) {
        isDone &= bol;
      }
      return isDone;
    }

    public boolean progress() {
      return true;
    }

    public void setMap(Map<Integer, List<Integer>> expectedIds) {
      for (Integer integer : expectedIds.keySet()) {
        Map<Integer, Boolean> perTarget = new ConcurrentHashMap<>();
        for (Integer integer1 : expectedIds.get(integer)) {
          perTarget.put(integer1, false);
        }
        finished.put(integer, perTarget);
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

  public static void main(String[] args) {
    // first load the configurations from command line and config files
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    // build JobConfig
    HashMap<String, Object> configurations = new HashMap<>();
    configurations.put(SchedulerContext.THREADS_PER_WORKER, 8);

    JobConfig jobConfig = new JobConfig();
    jobConfig.putAll(configurations);

    // build the job
    Twister2Job twister2Job = Twister2Job.newBuilder()
        .setName("basic-partition")
        .setWorkerClass(PartitionCommunication.class.getName())
        .setRequestResource(new WorkerComputeResource(2, 1024), 4)
        .setConfig(jobConfig)
        .build();

    // now submit the job
    Twister2Submitter.submitJob(twister2Job, config);
  }
}
