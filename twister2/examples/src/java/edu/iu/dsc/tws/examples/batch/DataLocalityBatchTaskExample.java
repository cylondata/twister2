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
package edu.iu.dsc.tws.examples.batch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Submitter;
import edu.iu.dsc.tws.api.basic.job.BasicJob;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.core.TWSNetwork;
import edu.iu.dsc.tws.data.api.HDFSConnector;
import edu.iu.dsc.tws.executor.ExecutionPlan;
import edu.iu.dsc.tws.executor.ExecutionPlanBuilder;
import edu.iu.dsc.tws.executor.threading.ExecutionModel;
import edu.iu.dsc.tws.executor.threading.ThreadExecutor;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
import edu.iu.dsc.tws.rsched.spi.container.IContainer;
import edu.iu.dsc.tws.rsched.spi.resource.ResourceContainer;
import edu.iu.dsc.tws.rsched.spi.resource.ResourcePlan;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.Operations;
import edu.iu.dsc.tws.task.api.SinkTask;
import edu.iu.dsc.tws.task.api.SourceTask;
import edu.iu.dsc.tws.task.api.TaskContext;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.GraphBuilder;
import edu.iu.dsc.tws.task.graph.GraphConstants;
import edu.iu.dsc.tws.tsched.batch.datalocality.DataLocalityBatchTaskScheduling;
import edu.iu.dsc.tws.tsched.spi.common.TaskSchedulerContext;
import edu.iu.dsc.tws.tsched.spi.scheduler.Worker;
import edu.iu.dsc.tws.tsched.spi.scheduler.WorkerPlan;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedulePlan;

public class DataLocalityBatchTaskExample implements IContainer {

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

    BasicJob.BasicJobBuilder jobBuilder = BasicJob.newBuilder();
    jobBuilder.setName("complex-task-datalocality-example");
    jobBuilder.setContainerClass(DataLocalityBatchTaskExample.class.getName());
    jobBuilder.setRequestResource(new ResourceContainer(2, 1024), 2);
    jobBuilder.setConfig(jobConfig);

    // now submit the job
    Twister2Submitter.submitContainerJob(jobBuilder.build(), config);
  }

  @Override
  public void init(Config config, int id, ResourcePlan resourcePlan) {

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

    builder.connect("source", "sink1", "partition-edge1", Operations.PARTITION);
    builder.connect("sink1", "sink2", "partition-edge2", Operations.PARTITION);
    builder.connect("sink1", "merge", "partition-edge3", Operations.PARTITION);
    builder.connect("sink2", "final", "partition-edge4", Operations.PARTITION);
    builder.connect("merge", "final", "partition-edge5", Operations.PARTITION);

     /*builder.connect("source", "sink1", "partition-edge1", Operations.PARTITION);
    builder.connect("source", "sink2", "partition-edge2", Operations.PARTITION);
    builder.connect("sink1", "merge", "partition-edge3", Operations.PARTITION);
    builder.connect("sink2", "merge", "partition-edge4", Operations.PARTITION);
    builder.connect("merge", "final", "partition-edge5", Operations.PARTITION);*/

    builder.addConfiguration("source", "Ram", GraphConstants.taskInstanceRam(config));
    builder.addConfiguration("source", "Disk", GraphConstants.taskInstanceDisk(config));
    builder.addConfiguration("source", "Cpu", GraphConstants.taskInstanceCpu(config));

    List<String> sourceInputDataset = new ArrayList<>();
    sourceInputDataset.add("dataset1.txt");
    sourceInputDataset.add("dataset2.txt");

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
    WorkerPlan workerPlan = createWorkerPlan(resourcePlan);

    String jobType = "Batch";
    List<TaskSchedulePlan> taskSchedulePlanList = new ArrayList<>();
    TaskSchedulePlan taskSchedulePlan = null;

    if (id == 0) { //Remove this condition during Executor integration
      if ("batch".equalsIgnoreCase(jobType)
          && TaskSchedulerContext.taskSchedulingMode(config).equals("datalocalityaware")) {
        DataLocalityBatchTaskScheduling dataLocalityBatchTaskScheduling = new
            DataLocalityBatchTaskScheduling();
        dataLocalityBatchTaskScheduling.initialize(config);
        taskSchedulePlanList = dataLocalityBatchTaskScheduling.scheduleBatch(graph, workerPlan);
      }
    }

    //Just to print the task schedule plan.
    if (id == 0) {
      for (int j = 0; j < taskSchedulePlanList.size(); j++) {
        taskSchedulePlan = taskSchedulePlanList.get(j);
        Map<Integer, TaskSchedulePlan.ContainerPlan> containersMap
            = taskSchedulePlan.getContainersMap();
        LOG.info("Task Schedule Plan:" + j);
        for (Map.Entry<Integer, TaskSchedulePlan.ContainerPlan> entry : containersMap.entrySet()) {
          Integer integer = entry.getKey();
          TaskSchedulePlan.ContainerPlan containerPlan = entry.getValue();
          Set<TaskSchedulePlan.TaskInstancePlan> taskContainerPlan
              = containerPlan.getTaskInstances();
          for (TaskSchedulePlan.TaskInstancePlan ip : taskContainerPlan) {
            LOG.info("\tTask Id:" + ip.getTaskId() + "\tTask Index:" + ip.getTaskIndex()
                + "\tTask Name:" + ip.getTaskName() + "\tContainer Id:" + integer);
          }
        }
      }
    }

    TWSNetwork network = new TWSNetwork(config, resourcePlan.getThisId());
    ExecutionPlanBuilder executionPlanBuilder = new ExecutionPlanBuilder(resourcePlan, network);
    ExecutionPlan plan = executionPlanBuilder.schedule(config, graph, taskSchedulePlan);
    ExecutionModel executionModel = new ExecutionModel(ExecutionModel.SHARED);
    ThreadExecutor executor = new ThreadExecutor(executionModel, plan, network.getChannel());
    executor.execute();
  }

  public WorkerPlan createWorkerPlan(ResourcePlan resourcePlan) {
    List<Worker> workers = new ArrayList<>();
    for (ResourceContainer resource : resourcePlan.getContainers()) {
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

  private static class SourceTask1 extends SourceTask {
    private static final long serialVersionUID = -254264903510284748L;
    private TaskContext ctx;

    @Override
    public void run() {
      ctx.write("partition-edge", "Hello");
    }

    @Override
    public void prepare(Config cfg, TaskContext context) {
      this.ctx = context;
    }
  }

  private static class SourceTask2 extends SourceTask {
    private static final long serialVersionUID = -254264903510284748L;
    private TaskContext ctx;

    @Override
    public void run() {
      ctx.write("partition-edge", "Hello");
    }

    @Override
    public void prepare(Config cfg, TaskContext context) {
      this.ctx = context;
    }
  }

  private static class ReceivingTask extends SinkTask {
    private static final long serialVersionUID = -254264903510284798L;
    private int count = 0;
    private TaskContext ctx;
    private Config config;
    private String outputFile;
    private String inputFile;
    private HDFSConnector hdfsConnector = null;

    @Override
    public void execute(IMessage message) {

      LOG.info("Message Partition Received : " + message.getContent()
          + ", Count : " + count);
      hdfsConnector.HDFSConnect(message.getContent().toString());
      count++;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void prepare(Config cfg, TaskContext context) {
      this.ctx = context;
      this.config = cfg;

      Map<String, Object> configs = context.getConfigurations();
      for (Map.Entry<String, Object> entry : configs.entrySet()) {
        if (entry.getKey().toString().contains("outputdataset")) {
          List<String> outputFiles = (List<String>) entry.getValue();
          for (int i = 0; i < outputFiles.size(); i++) {
            this.outputFile = outputFiles.get(i);
            LOG.info("Output File(s):" + this.outputFile);
          }
        }
        hdfsConnector = new HDFSConnector(config, outputFile);
      }
    }
  }

  private static class MergingTask extends SinkTask {
    private static final long serialVersionUID = -254264903510284798L;
    private int count = 0;
    private TaskContext ctx;
    private Config config;
    private String outputFile;
    private String inputFile;
    private HDFSConnector hdfsConnector = null;

    @Override
    public void execute(IMessage message) {

      LOG.info("Message Partition Received : " + message.getContent()
          + ", Count : " + count);
      count++;
    }

    @Override
    public void prepare(Config cfg, TaskContext context) {
      this.ctx = context;
      this.config = cfg;
    }
  }

  private static class FinalTask extends SinkTask {
    private static final long serialVersionUID = -254264903510284798L;
    private int count = 0;
    private TaskContext ctx;
    private Config config;
    private String outputFile;
    private String inputFile;
    private HDFSConnector hdfsConnector = null;

    @Override
    public void execute(IMessage message) {

      LOG.info("Message Partition Received : " + message.getContent()
          + ", Count : " + count);
      count++;
    }

    @Override
    public void prepare(Config cfg, TaskContext context) {
      this.ctx = context;
      this.config = cfg;
    }
  }
}
