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

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Submitter;
import edu.iu.dsc.tws.api.basic.job.BasicJob;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.api.ReduceFunction;
import edu.iu.dsc.tws.comms.api.ReduceReceiver;
import edu.iu.dsc.tws.comms.core.TWSCommunication;
import edu.iu.dsc.tws.comms.core.TWSNetwork;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.examples.IntData;
import edu.iu.dsc.tws.examples.Utils;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.spi.container.IContainer;
import edu.iu.dsc.tws.rsched.spi.resource.ResourceContainer;
import edu.iu.dsc.tws.rsched.spi.resource.ResourcePlan;

public class BaseAllReduceCommunication implements IContainer {
  private static final Logger LOG = Logger.getLogger(BaseAllReduceCommunication.class.getName());

  private DataFlowOperation allReduce;

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
    int dest = NO_OF_TASKS;

    Map<String, Object> newCfg = new HashMap<>();

    LOG.info("Setting up reduce dataflow operation");
    try {
      // this method calls the init method
      // I think this is wrong
      allReduce = channel.allReduce(newCfg, MessageType.OBJECT, 0, 1, sources,
          destinations, dest, new IndentityFunction(), new FinalReduceReceive(), true);

      if (id == 0 || id == 1) {
        for (int i = 0; i < noOfTasksPerExecutor; i++) {
          // the map thread where data is produced
          LOG.info(String.format("%d Starting %d", id, i + id * noOfTasksPerExecutor));
          Thread mapThread = new Thread(new MapWorker(i + id * noOfTasksPerExecutor));
          mapThread.start();
        }
      }
      // we need to progress the communication
      while (true) {
        try {
          // progress the channel
          channel.progress();
          // we should progress the communication directive
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
        status = Status.MAP_FINISHED;
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

  private class FinalReduceReceive implements ReduceReceiver {
    private int count = 0;
    public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
      for (Map.Entry<Integer, List<Integer>> e : expectedIds.entrySet()) {
        LOG.info(String.format("%d Final Task %d receives from %s",
            id, e.getKey(), e.getValue().toString()));
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
    JobConfig jobConfig = new JobConfig();

    // build the job
    BasicJob basicJob = BasicJob.newBuilder()
        .setName("basic-all-reduce")
        .setContainerClass(BaseAllReduceCommunication.class.getName())
        .setRequestResource(new ResourceContainer(2, 1024), 4)
        .setConfig(jobConfig)
        .build();

    // now submit the job
    Twister2Submitter.submitContainerJob(basicJob, config);
  }
}
