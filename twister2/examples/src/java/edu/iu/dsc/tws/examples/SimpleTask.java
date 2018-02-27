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

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Submitter;
import edu.iu.dsc.tws.api.basic.job.BasicJob;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.api.ReduceFunction;
import edu.iu.dsc.tws.comms.api.ReduceReceiver;
import edu.iu.dsc.tws.comms.core.TWSCommunication;
import edu.iu.dsc.tws.comms.core.TWSNetwork;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.spi.container.IContainer;
import edu.iu.dsc.tws.rsched.spi.resource.ResourceContainer;
import edu.iu.dsc.tws.rsched.spi.resource.ResourcePlan;

public class SimpleTask implements IContainer {

  private static final Logger LOG = Logger.getLogger(SimpleTask.class.getName());

  private DataFlowOperation direct;

  private ResourcePlan resourcePlan;

  private int id;

  private Config config;

  private static final int NO_OF_TASKS = 2;

  private int noOfTasksPerExecutor = 1;

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
    for (int i = 0; i < NO_OF_TASKS; i++) {
      sources.add(i);
    }
    int dest = NO_OF_TASKS;

    Map<String, Object> newCfg = new HashMap<>();

    LOG.info("Setting up Task dataflow operation");
    try {
      // this method calls the init method
      // I think this is wrong
     /* direct = channel.reduce(newCfg, MessageType.OBJECT, 0, sources,
          dest, new ReduceStreamingFinalReceiver(new IdentityFunction(), new FinalReduceReceiver()),
          new ReduceStreamingPartialReceiver(dest, new IdentityFunction()));*/

      direct = channel.direct(newCfg, MessageType.OBJECT, 0, sources,
          dest, new PingPongReceive());

      for (int i = 0; i < noOfTasksPerExecutor; i++) {
        // the map thread where data is produced
        LOG.info(String.format("%d Starting %d", id, i + id * noOfTasksPerExecutor));
        Thread mapThread = new Thread(new MapWorker(i + id * noOfTasksPerExecutor));
        mapThread.start();
      }
      // we need to progress the communication
      while (true) {
        try {
          // progress the channel
          channel.progress();
          // we should progress the communication directive
          direct.progress();
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
        IntData data = generateData();
        for (int i = 0; i < 10; i++) {
          // lets generate a message
          while (!direct.send(task, data, 0)) {
            // lets wait a litte and try again
            direct.progress();
            Thread.yield();
//            Thread.sleep(1);
          }
//          LOG.info(String.format("%d sending to %d", id, task)
//              + " count: " + sendCount++);
          if (i % 2 == 0 || i > 8) {
            LOG.info(String.format("%d sent %d", id, i));
          }
        }

        LOG.info(String.format("%d Done sending", id));
        status = Status.MAP_FINISHED;
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
    int s = 64000;
    int[] d = new int[s];
    for (int i = 0; i < s; i++) {
      d[i] = i;
    }
    return new IntData(d);
  }

  public static class FinalReduceReceiver implements ReduceReceiver {
    private int count = 0;
    @Override
    public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {

    }

    @Override
    public boolean receive(int target, Object object) {
      count++;
      if (count > 4900 || count % 10 == 0) {
        LOG.info(String.format("Final ReducerReceive Received %d", count));
      }
      return true;
    }
  }

  public static class IdentityFunction implements ReduceFunction {
    @Override
    public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    }

    @Override
    public Object reduce(Object t1, Object t2) {
      return t1;
    }
  }

  private class PingPongReceive implements MessageReceiver {
    private int count = 0;

    @Override
    public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
      LOG.info("Dataflow Operation : " + op.toString());
      LOG.info("Expected Ids Size : " + expectedIds.size());
      LOG.info("Expected Ids : " + expectedIds);
    }

    @Override
    public boolean onMessage(int source, int path, int target, int flags, Object object) {
      count++;
      LOG.info("-------------------------------------------");
      LOG.info("Received message: " + count);
      LOG.info("-------------------------------------------");

      if (count % 7 == 0) {
        LOG.info("-------------------------------------------");
        LOG.info("Special received message: " + count);
        LOG.info("-------------------------------------------");
      }

      if (count == 10) {
        status = Status.LOAD_RECEIVE_FINISHED;
      }
      return true;
    }

    @Override
    public void progress() {

    }
  }


  public static void main(String[] args) {
    // first load the configurations from command line and config files
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    // build JobConfig
    JobConfig jobConfig = new JobConfig();

    // build the job
    BasicJob basicJob = BasicJob.newBuilder()
        .setName("basic-simple-task")
        .setContainerClass(SimpleTask.class.getName())
        .setRequestResource(new ResourceContainer(2, 1024), 2)
        .setConfig(jobConfig)
        .build();

    // now submit the job
    Twister2Submitter.submitContainerJob(basicJob, config);
  }
}










