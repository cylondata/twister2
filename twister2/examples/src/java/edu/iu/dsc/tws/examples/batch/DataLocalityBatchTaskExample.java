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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Submitter;
import edu.iu.dsc.tws.api.basic.job.BasicJob;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.core.TWSNetwork;
import edu.iu.dsc.tws.comms.utils.KryoSerializer;
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
import edu.iu.dsc.tws.tsched.batch.roundrobin.RoundRobinBatchTaskScheduling;
import edu.iu.dsc.tws.tsched.spi.common.TaskSchedulerContext;
import edu.iu.dsc.tws.tsched.spi.scheduler.Worker;
import edu.iu.dsc.tws.tsched.spi.scheduler.WorkerPlan;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedulePlan;

public class DataLocalityBatchTaskExample implements IContainer {

  private static final Logger LOG =
      Logger.getLogger(DataLocalityBatchTaskExample.class.getName());

  private FileHandler fileHandler;

  public static void main(String[] args) {
    // first load the configurations from command line and config files
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    // build JobConfig
    HashMap<String, byte[]> objectHashMap = new HashMap<>();
    objectHashMap.put(SchedulerContext.THREADS_PER_WORKER, new KryoSerializer().serialize(8));
    // build JobConfig
    JobConfig jobConfig = new JobConfig();
    jobConfig.putAll(objectHashMap);

    BasicJob.BasicJobBuilder jobBuilder = BasicJob.newBuilder();
    jobBuilder.setName("hdfs-task-datalocality-example");
    jobBuilder.setContainerClass(DataLocalityBatchTaskExample.class.getName());
    jobBuilder.setRequestResource(new ResourceContainer(2, 1024), 2);
    jobBuilder.setConfig(jobConfig);

    // now submit the job
    Twister2Submitter.submitContainerJob(jobBuilder.build(), config);
  }

  @Override
  public void init(Config config, int id, ResourcePlan resourcePlan) {

    try {
      fileHandler = new FileHandler("/home/kgovind/twister2/taskscheduler.log");
      LOG.addHandler(fileHandler);
      SimpleFormatter simpleFormatter = new SimpleFormatter();
      fileHandler.setFormatter(simpleFormatter);
    } catch (IOException ioe) {
      ioe.printStackTrace();
    }

    GeneratorTask g = new GeneratorTask();
    ReceivingTask r = new ReceivingTask();

    MergingTask m = new MergingTask();

    GraphBuilder builder = GraphBuilder.newBuilder();
    builder.addSource("source", g);
    builder.setParallelism("source", 4);
    builder.addSink("sink", r);
    builder.setParallelism("sink", 3);
    builder.connect("source", "sink", "partition-edge", Operations.PARTITION);

    builder.addSink("merge", m);
    builder.setParallelism("merge", 3);
    builder.connect("source", "merge", "partition-edge", Operations.PARTITION);

    //Adding source task property configurations
    List<String> sourceInputDataset = new ArrayList<>();
    sourceInputDataset.add("dataset1.txt");
    sourceInputDataset.add("dataset2.txt");

    builder.addConfiguration("source", "Ram", GraphConstants.taskInstanceRam(config));
    builder.addConfiguration("source", "Disk", GraphConstants.taskInstanceDisk(config));
    builder.addConfiguration("source", "Cpu", GraphConstants.taskInstanceCpu(config));
    builder.addConfiguration("source", "inputdataset", sourceInputDataset);

    List<String> sourceOutputDataset = new ArrayList<>();
    sourceOutputDataset.add("sourceoutput.txt");
    builder.addConfiguration("source", "outputdataset", sourceOutputDataset);

    //Adding sink task property configurations
    List<String> sinkInputDataset = new ArrayList<>();
    sinkInputDataset.add("dataset3.txt");
    sinkInputDataset.add("dataset4.txt");

    builder.addConfiguration("sink", "Ram", GraphConstants.taskInstanceRam(config));
    builder.addConfiguration("sink", "Disk", GraphConstants.taskInstanceDisk(config));
    builder.addConfiguration("sink", "Cpu", GraphConstants.taskInstanceCpu(config));
    builder.addConfiguration("sink", "inputdataset", sinkInputDataset);

    List<String> sinkOutputDataset = new ArrayList<>();
    sinkOutputDataset.add("sinkoutput.txt");
    builder.addConfiguration("sink", "outputdataset", sinkOutputDataset);

    //Adding merge property
    List<String> mergeInputDataset = new ArrayList<>();
    mergeInputDataset.add("dataset3.txt");
    mergeInputDataset.add("dataset4.txt");

    builder.addConfiguration("sink", "Ram", GraphConstants.taskInstanceRam(config));
    builder.addConfiguration("sink", "Disk", GraphConstants.taskInstanceDisk(config));
    builder.addConfiguration("sink", "Cpu", GraphConstants.taskInstanceCpu(config));
    builder.addConfiguration("sink", "inputdataset", sinkInputDataset);

    List<String> mergeOutputDataset = new ArrayList<>();
    mergeOutputDataset.add("sinkoutput.txt");
    builder.addConfiguration("sink", "outputdataset", mergeOutputDataset);

    DataFlowTaskGraph graph = builder.build();
    WorkerPlan workerPlan = createWorkerPlan(resourcePlan);

    List<TaskSchedulePlan> taskSchedulePlanList = new ArrayList<>();
    TaskSchedulePlan taskSchedulePlan = null;

    String jobType = "batch";

    //if (id == 0) {
    if ("batch".equals(jobType)
        && TaskSchedulerContext.taskSchedulingMode(config).equals("datalocalityaware")) {
      DataLocalityBatchTaskScheduling dataLocalityBatchTaskScheduling = new
          DataLocalityBatchTaskScheduling();
      dataLocalityBatchTaskScheduling.initialize(config);
      taskSchedulePlanList = dataLocalityBatchTaskScheduling.scheduleBatch(graph, workerPlan);
    } else if (TaskSchedulerContext.taskSchedulingMode(config).equals("roundrobin")) {
      RoundRobinBatchTaskScheduling rrBatchTaskScheduling = new RoundRobinBatchTaskScheduling();
      rrBatchTaskScheduling.initialize(config);
      taskSchedulePlanList = rrBatchTaskScheduling.scheduleBatch(graph, workerPlan);
    }

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
    //}

    TWSNetwork network = new TWSNetwork(config, resourcePlan.getThisId());
    ExecutionPlanBuilder executionPlanBuilder = new ExecutionPlanBuilder(resourcePlan, network);
    ExecutionPlan plan = executionPlanBuilder.schedule(config, graph, taskSchedulePlan);
    ExecutionModel executionModel = new ExecutionModel(ExecutionModel.SHARED);
    ThreadExecutor executor = new ThreadExecutor(executionModel, plan);
    executor.execute();

    // we need to progress the channel
    while (true) {
      network.getChannel().progress();
    }
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

  private static class GeneratorTask extends SourceTask {
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
}
