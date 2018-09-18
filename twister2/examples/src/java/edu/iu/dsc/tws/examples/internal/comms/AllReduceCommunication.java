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
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.api.ReduceFunction;
import edu.iu.dsc.tws.comms.api.SingularReceiver;
import edu.iu.dsc.tws.comms.api.TWSChannel;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.dfw.DataFlowAllReduce;
import edu.iu.dsc.tws.examples.IntData;
import edu.iu.dsc.tws.examples.Utils;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;

public class AllReduceCommunication implements IWorker {
  private static final Logger LOG = Logger.getLogger(AllReduceCommunication.class.getName());

  private DataFlowAllReduce allReduce;

  private int id;

  private static final int NO_OF_TASKS = 16;

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
    for (int i = 0; i < NO_OF_TASKS / 2; i++) {
      sources.add(i);
    }
    Set<Integer> destinations = new HashSet<>();
    for (int i = 0; i < NO_OF_TASKS / 2; i++) {
      destinations.add(NO_OF_TASKS / 2 + i);
    }
    int dest = NO_OF_TASKS;

    Map<String, Object> newCfg = new HashMap<>();

    LOG.info("Setting up reduce dataflow operation");
    try {
      // this method calls the execute method
      // I think this is wrong
      allReduce = new DataFlowAllReduce(network, sources, destinations, dest,
          new IndentityFunction(), new FinalSingularReceive(), 0, 1, true);
      allReduce.init(cfg, MessageType.OBJECT, taskPlan, 0);

      if (id == 0 || id == 1) {
        for (int i = 0; i < noOfTasksPerExecutor; i++) {
          // the map thread where data is produced
          LOG.info(String.format("%d Starting %d", id, i + id * noOfTasksPerExecutor));
          Thread mapThread = new Thread(new MapWorker(i + id * noOfTasksPerExecutor));
          mapThread.start();
        }
      }
      // we need to communicationProgress the communication
      while (true) {
        try {
          // communicationProgress the channel
          network.progress();
          // we should communicationProgress the communication directive
          allReduce.progress();
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
    MapWorker(int task) {
      this.task = task;
    }

    @Override
    public void run() {
      try {
        LOG.log(Level.INFO, "Starting map worker: " + id);
        IntData data = generateData();
        for (int i = 0; i < 10000; i++) {
          // lets generate a message
          while (!allReduce.send(task, data, 0)) {
            // lets wait a litte and try again
            try {
              Thread.sleep(1);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
          }
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

  private class IndentityFunction implements ReduceFunction {
    private int count = 0;
    @Override
    public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    }

    @Override
    public Object reduce(Object t1, Object t2) {
      count++;
      if (count % 100 == 0) {
        LOG.info(String.format("%d Count %d", id, count));
      }
      return t1;
    }
  }

  private class FinalSingularReceive implements SingularReceiver {
    private int count = 0;
    public void init(Config cfg, Set<Integer> expectedIds) {
      for (Integer e : expectedIds) {
        LOG.info(String.format("%d Final Task receives from %d", id, e));
      }
    }

    @Override
    public boolean receive(int target, Object object) {
      count++;
      if (count % 100 == 0) {
        LOG.info("Message received for last target: "
            + target + " count: " + count);
      }
      return true;
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

    // build JobConfig
    JobConfig jobConfig = new JobConfig();
    jobConfig.putAll(configurations);

    // build the job
    Twister2Job twister2Job = Twister2Job.newBuilder()
        .setName("basic-all-reduce")
        .setWorkerClass(AllReduceCommunication.class.getName())
        .setRequestResource(new WorkerComputeResource(2, 1024), 4)
        .setConfig(jobConfig)
        .build();

    // now submit the job
    Twister2Submitter.submitJob(twister2Job, config);
  }
}
