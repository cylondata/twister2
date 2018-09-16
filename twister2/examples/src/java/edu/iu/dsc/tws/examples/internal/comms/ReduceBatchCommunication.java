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
import java.util.stream.IntStream;

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
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.api.ReduceFunction;
import edu.iu.dsc.tws.comms.api.SingularReceiver;
import edu.iu.dsc.tws.comms.api.TWSChannel;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.dfw.DataFlowReduce;
import edu.iu.dsc.tws.comms.dfw.io.reduce.ReduceBatchFinalReceiver;
import edu.iu.dsc.tws.comms.dfw.io.reduce.ReduceBatchPartialReceiver;
import edu.iu.dsc.tws.examples.IntData;
import edu.iu.dsc.tws.examples.Utils;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;

public class ReduceBatchCommunication implements IWorker {
  private static final Logger LOG = Logger.getLogger(ReduceBatchCommunication.class.getName());

  private DataFlowReduce reduce;

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
    for (int i = 0; i < NO_OF_TASKS; i++) {
      sources.add(i);
    }
    int dest = NO_OF_TASKS;

    LOG.info("Setting up reduce dataflow operation");
    // this method calls the execute method
    // I think this is wrong
    reduce = new DataFlowReduce(network, sources,
        dest, new ReduceBatchFinalReceiver(new SumFunction(), new FinalSingularReceiver()),
        new ReduceBatchPartialReceiver(dest, new SumFunction()));
    reduce.init(cfg, MessageType.OBJECT, taskPlan, 0);

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
        reduce.progress();
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
        IntData data = generateData();
        for (int i = 0; i < 10; i++) {
          // lets generate a message
          int flag = 0;
          if (i == 10 - 1) {
            flag = MessageFlags.LAST;
          }
          while (!reduce.send(task, data, flag)) {
            // lets wait a litte and try again
            reduce.progress();
            Thread.yield();
//            try {
//            Thread.sleep(1);
//            } catch (InterruptedException e) {
//              e.printStackTrace();
//            }
          }
          if (i % 10 == 0) {
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

  /**
   * Generate data with an integer array
   *
   * @return IntData
   */
  private IntData generateData() {
    int s = 64000;
    int[] d = new int[s];
    for (int i = 0; i < s; i++) {
      d[i] = 1;
    }
    return new IntData(d);
  }

  public static class FinalSingularReceiver implements SingularReceiver {
    private int count = 0;

    @Override
    public void init(Config cfg, Set<Integer> expectedIds) {

    }

    @Override
    public boolean receive(int target, Object object) {
      count++;
      if (count % 1 == 0) {
        LOG.info(String.format("%d Received %d", target, count));
        LOG.info(String.format("%d Received Value %d", target, ((IntData) object).getData()[1]));
      }
      return true;
    }
  }

  public static class IdentityFunction implements ReduceFunction {
    private int count = 0;

    @Override
    public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    }

    @Override
    public Object reduce(Object t1, Object t2) {
      count++;
      if (count % 10 == 0) {
        LOG.info(String.format("Partial received %d", count));
      }
      return t1;
    }
  }

  public static class SumFunction implements ReduceFunction {

    @Override
    public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {

    }

    @Override
    public Object reduce(Object t1, Object t2) {
      IntData d1 = (IntData) t1;
      IntData d2 = (IntData) t2;
      int[] sum = new int[d1.getData().length];
      IntStream.range(0, d1.getData().length).forEach(i -> sum[i] = d1.getData()[i]
          + d2.getData()[i]);

      return new IntData(sum);
    }
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
        .setName("basic-batch-reduce")
        .setWorkerClass(ReduceBatchCommunication.class.getName())
        .setRequestResource(new WorkerComputeResource(1, 1024), 4)
        .setConfig(jobConfig)
        .build();

    // now submit the job
    Twister2Submitter.submitJob(twister2Job, config);
  }
}
