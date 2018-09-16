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
package edu.iu.dsc.tws.examples.internal.batch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import edu.iu.dsc.tws.comms.api.TWSChannel;
import edu.iu.dsc.tws.comms.op.Communicator;
import edu.iu.dsc.tws.data.utils.HdfsUtils;
import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.executor.core.ExecutionPlanBuilder;
import edu.iu.dsc.tws.executor.core.OperationNames;
import edu.iu.dsc.tws.executor.threading.Executor;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.TaskContext;
import edu.iu.dsc.tws.task.batch.BaseBatchSink;
import edu.iu.dsc.tws.task.batch.BaseBatchSource;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.GraphBuilder;
import edu.iu.dsc.tws.task.graph.GraphConstants;
import edu.iu.dsc.tws.task.graph.OperationMode;
import edu.iu.dsc.tws.tsched.spi.scheduler.Worker;
import edu.iu.dsc.tws.tsched.spi.scheduler.WorkerPlan;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedulePlan;
import edu.iu.dsc.tws.tsched.taskscheduler.TaskScheduler;

public class DataLocalityBatchTaskExample implements IWorker {

  private static final Logger LOG =
          Logger.getLogger(DataLocalityBatchTaskExample.class.getName());

  public static void main(String[] args) {
    // first load the configurations from command line and config files
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    // build JobConfig
    HashMap<String, Object> configurations = new HashMap<>();
    configurations.put(SchedulerContext.THREADS_PER_WORKER, 8);

    // build JobConfig
    JobConfig jobConfig = new JobConfig();
    jobConfig.putAll(configurations);

    Twister2Job.BasicJobBuilder jobBuilder = Twister2Job.newBuilder();
    jobBuilder.setName("datalocality-batchexample");
    jobBuilder.setWorkerClass(DataLocalityBatchTaskExample.class.getName());
    jobBuilder.setRequestResource(new WorkerComputeResource(2, 1024), 2);
    jobBuilder.setConfig(jobConfig);

    // now submit the job
    Twister2Submitter.submitJob(jobBuilder.build(), config);
  }

  @Override
  public void execute(Config config, int workerID, AllocatedResources resources,
                      IWorkerController workerController,
                      IPersistentVolume persistentVolume,
                      IVolatileVolume volatileVolume) {

    SourceTask1 g = new SourceTask1(); //source task
    SourceTask2 m = new SourceTask2(); //sink task 1
    ReceivingTask r = new ReceivingTask(); //sink task2
    MergingTask m1 = new MergingTask(); //merge task
    FinalTask f = new FinalTask(); //final task

    GraphBuilder builder = GraphBuilder.newBuilder();

    builder.addSource("source", g);
    builder.setParallelism("source", 4);

    builder.addSource("sink1", m);
    builder.setParallelism("sink1", 3);

    builder.addSink("sink2", r);
    builder.setParallelism("sink2", 3);

    builder.addSink("merge", m1);
    builder.setParallelism("merge", 3);

    builder.addSink("final", f);
    builder.setParallelism("final", 4);

    //Task graph Structure
    /**   Source
     *      |
     *      V
     *    Sink1 (Two Outgoing Edges)
     *   |     |
     *   V     V
     * Sink2  Merge
     *      |
     *      V
     *    Final
     */

    builder.connect("source", "sink1", "partition-edge1",
            OperationNames.PARTITION);
    builder.connect("sink1", "sink2", "partition-edge2",
            OperationNames.PARTITION);
    builder.connect("sink1", "merge", "partition-edge3",
            OperationNames.PARTITION);
    builder.connect("sink2", "final", "partition-edge4",
            OperationNames.PARTITION);
    builder.connect("merge", "final", "partition-edge5",
            OperationNames.PARTITION);

    builder.operationMode(OperationMode.BATCH);

    builder.addConfiguration("source", "Ram", GraphConstants.taskInstanceRam(config));
    builder.addConfiguration("source", "Disk", GraphConstants.taskInstanceDisk(config));
    builder.addConfiguration("source", "Cpu", GraphConstants.taskInstanceCpu(config));

    List<String> sourceInputDataset = new ArrayList<>();
    sourceInputDataset.add("dataset1.txt");

    builder.addConfiguration("source", "inputdataset", sourceInputDataset);
    builder.addConfiguration("sink1", "inputdataset", sourceInputDataset);
    builder.addConfiguration("sink2", "inputdataset", sourceInputDataset);
    builder.addConfiguration("merge", "inputdataset", sourceInputDataset);
    builder.addConfiguration("final", "inputdataset", sourceInputDataset);

    List<String> sinkOutputDataset1 = new ArrayList<>();
    sinkOutputDataset1.add("sinkoutput1.txt");
    builder.addConfiguration("sink1", "outputdataset1", sinkOutputDataset1);

    List<String> sinkOutputDataset2 = new ArrayList<>();
    sinkOutputDataset2.add("sinkoutput2.txt");
    builder.addConfiguration("sink2", "outputdataset2", sinkOutputDataset2);

    DataFlowTaskGraph graph = builder.build();
    WorkerPlan workerPlan = createWorkerPlan(resources);

    //Assign the "datalocalityaware" or "roundrobin" scheduling mode in config file.
    TaskScheduler taskScheduler = new TaskScheduler();
    taskScheduler.initialize(config);
    TaskSchedulePlan taskSchedulePlan = taskScheduler.schedule(graph, workerPlan);

    //Just to print the task schedule plan...
    if (workerID == 0) {
      if (taskSchedulePlan != null) {
        Map<Integer, TaskSchedulePlan.ContainerPlan> containersMap
                = taskSchedulePlan.getContainersMap();
        for (Map.Entry<Integer, TaskSchedulePlan.ContainerPlan> entry : containersMap.entrySet()) {
          Integer integer = entry.getKey();
          TaskSchedulePlan.ContainerPlan containerPlan = entry.getValue();
          Set<TaskSchedulePlan.TaskInstancePlan> containerPlanTaskInstances
                  = containerPlan.getTaskInstances();
          LOG.info("Task Details for Container Id:" + integer);
          for (TaskSchedulePlan.TaskInstancePlan ip : containerPlanTaskInstances) {
            LOG.info("Task Id:" + ip.getTaskId()
                    + "\tTask Index" + ip.getTaskIndex()
                    + "\tTask Name:" + ip.getTaskName());
          }
        }
      }
    }

    TWSChannel network = Network.initializeChannel(config, workerController, resources);
    ExecutionPlanBuilder executionPlanBuilder = new ExecutionPlanBuilder(resources,
            new Communicator(config, network));
    ExecutionPlan plan = executionPlanBuilder.build(config, graph, taskSchedulePlan);
    Executor executor = new Executor(config, workerID, plan, network, OperationMode.BATCH);
    executor.execute();
  }

  public WorkerPlan createWorkerPlan(AllocatedResources resourcePlan) {
    List<Worker> workers = new ArrayList<>();
    for (WorkerComputeResource resource : resourcePlan.getWorkerComputeResources()) {
      Worker w = new Worker(resource.getId());
      if (w.getId() == 0) {
        w.addProperty("bandwidth", 1000.0);
        w.addProperty("latency", 0.1);
      } else if (w.getId() == 1) {
        w.addProperty("bandwidth", 2000.0);
        w.addProperty("latency", 0.2);
      } else {
        w.addProperty("bandwidth", 3000.0);
        w.addProperty("latency", 0.3);
      }
      workers.add(w);
    }

    return new WorkerPlan(workers);
  }

  private static class SourceTask1 extends BaseBatchSource {
    private static final long serialVersionUID = -254264903510284748L;

    @Override
    public void execute() {
      context.write("partition-edge1", "Hello");
    }
  }

  private static class SourceTask2 extends BaseBatchSource {
    private static final long serialVersionUID = -254264903510284748L;

    @Override
    public void execute() {
      context.write("partition-edge2", "Hello");
    }
  }

  private static class ReceivingTask extends BaseBatchSink {
    private static final long serialVersionUID = -254264903510284798L;
    private int count = 0;
    private Config config;
    private String outputFile;
    private String inputFile;
    private HdfsUtils hdfsUtils = null;

    @Override
    public boolean execute(IMessage message) {
      LOG.info("Message AllReduced : " + message.getContent() + ", Count : " + count);
      count++;
      return true;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void prepare(Config cfg, TaskContext ctx) {
      this.context = ctx;
      this.config = cfg;

      Map<String, Object> configs = ctx.getConfigurations();
      for (Map.Entry<String, Object> entry : configs.entrySet()) {
        if (entry.getKey().contains("outputdataset")) {
          List<String> outputFiles = (List<String>) entry.getValue();
          for (int i = 0; i < outputFiles.size(); i++) {
            this.outputFile = outputFiles.get(i);
            LOG.info("Output File(s):" + this.outputFile);
          }
        }
        hdfsUtils = new HdfsUtils(config, outputFile);
      }
    }
  }

  private static class MergingTask extends BaseBatchSink {
    private static final long serialVersionUID = -254264903510284798L;
    private int count = 0;

    @Override
    public boolean execute(IMessage message) {

      LOG.info("Message Partition Received : " + message.getContent()
              + ", Count : " + count);
      count++;
      return true;
    }
  }

  private static class FinalTask extends BaseBatchSink {
    private static final long serialVersionUID = -254264903510284798L;
    private int count = 0;

    @Override
    public boolean execute(IMessage message) {

      LOG.info("Message Partition Received : " + message.getContent()
              + ", Count : " + count);
      count++;
      return true;
    }
  }
}

