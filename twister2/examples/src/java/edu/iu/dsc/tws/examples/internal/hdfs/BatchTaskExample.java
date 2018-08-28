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
package edu.iu.dsc.tws.examples.internal.hdfs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Submitter;
import edu.iu.dsc.tws.api.job.Twister2Job;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.discovery.IWorkerController;
import edu.iu.dsc.tws.common.resource.AllocatedResources;
import edu.iu.dsc.tws.common.resource.WorkerComputeResource;
import edu.iu.dsc.tws.common.worker.IPersistentVolume;
import edu.iu.dsc.tws.common.worker.IVolatileVolume;
import edu.iu.dsc.tws.common.worker.IWorker;
import edu.iu.dsc.tws.comms.core.TWSNetwork;
import edu.iu.dsc.tws.data.api.HDFSConnector;
import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.executor.core.ExecutionPlanBuilder;
import edu.iu.dsc.tws.executor.threading.Executor;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.Operations;
import edu.iu.dsc.tws.task.api.SinkTask;
import edu.iu.dsc.tws.task.api.SourceTask;
import edu.iu.dsc.tws.task.api.TaskContext;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.GraphBuilder;
import edu.iu.dsc.tws.task.graph.GraphConstants;
import edu.iu.dsc.tws.tsched.batch.roundrobin.RoundRobinBatchTaskScheduler;
import edu.iu.dsc.tws.tsched.spi.scheduler.Worker;
import edu.iu.dsc.tws.tsched.spi.scheduler.WorkerPlan;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedulePlan;

public class BatchTaskExample implements IWorker {

  private static final Logger LOG =
      Logger.getLogger(BatchTaskExample.class.getName());

  public static void main(String[] args) {
    // first load the configurations from command line and config files
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    // build JobConfig
    HashMap<String, Object> configurations = new HashMap<>();
    configurations.put(SchedulerContext.THREADS_PER_WORKER, 8);

    JobConfig jobConfig = new JobConfig();
    jobConfig.putAll(configurations);

    Twister2Job.BasicJobBuilder jobBuilder = Twister2Job.newBuilder();
    jobBuilder.setName("task-example");
    jobBuilder.setWorkerClass(BatchTaskExample.class.getName());
    jobBuilder.setRequestResource(new WorkerComputeResource(2, 1024), 2);
    jobBuilder.setConfig(jobConfig);

    // now submit the job
    Twister2Submitter.submitContainerJob(jobBuilder.build(), config);
  }

  @Override
  public void execute(Config config, int workerID, AllocatedResources resources,
                      IWorkerController workerController,
                      IPersistentVolume persistentVolume,
                      IVolatileVolume volatileVolume) {
    GeneratorTask g = new GeneratorTask();
    ReceivingTask r = new ReceivingTask();

    GraphBuilder builder = GraphBuilder.newBuilder();
    builder.addSource("source", g);
    builder.setParallelism("source", 4);
    builder.addSink("sink", r);
    builder.setParallelism("sink", 3);
    builder.connect("source", "sink", "partition-edge", Operations.PARTITION);

    builder.addConfiguration("source", "Ram", GraphConstants.taskInstanceRam(config));
    builder.addConfiguration("source", "Disk", GraphConstants.taskInstanceDisk(config));
    builder.addConfiguration("source", "Cpu", GraphConstants.taskInstanceCpu(config));

    List<String> sourceInputDataset = new ArrayList<>();
    sourceInputDataset.add("dataset1.txt");
    sourceInputDataset.add("dataset2.txt");

    builder.addConfiguration("source", "inputdataset", sourceInputDataset);
    List<String> sinkOutputDataset = new ArrayList<>();
    sinkOutputDataset.add("sinkoutput1.txt");
    builder.addConfiguration("sink", "outputdataset", sinkOutputDataset);

    DataFlowTaskGraph graph = builder.build();

    String jobType = "Batch";
    String schedulingType = "roundrobin";
    List<TaskSchedulePlan> scheduleBatch = new ArrayList<>();
    TaskSchedulePlan taskSchedulePlan = null;

    if ("Batch".equalsIgnoreCase(jobType)
        && "roundrobin".equalsIgnoreCase(schedulingType)) {
      RoundRobinBatchTaskScheduler rrBatchTaskScheduling = new RoundRobinBatchTaskScheduler();
      rrBatchTaskScheduling.initialize(config);
      WorkerPlan workerPlan = createWorkerPlan(resources);
      scheduleBatch = rrBatchTaskScheduling.scheduleBatch(graph, workerPlan);
    }

    for (int j = 0; j < scheduleBatch.size(); j++) {
      taskSchedulePlan = scheduleBatch.get(j);
      Map<Integer, TaskSchedulePlan.ContainerPlan> containersMap
          = taskSchedulePlan.getContainersMap();
      for (Map.Entry<Integer, TaskSchedulePlan.ContainerPlan> entry : containersMap.entrySet()) {
        Integer integer = entry.getKey();
        TaskSchedulePlan.ContainerPlan containerPlan = entry.getValue();
        Set<TaskSchedulePlan.TaskInstancePlan> taskContainerPlan = containerPlan.getTaskInstances();
        for (TaskSchedulePlan.TaskInstancePlan ip : taskContainerPlan) {
          LOG.info("Task Id:" + ip.getTaskId() + "\tTask Index" + ip.getTaskIndex()
              + "\tTask Name:" + ip.getTaskName() + "\tContainer Id:" + integer);
        }
      }
    }

    TWSNetwork network = new TWSNetwork(config, resources.getWorkerId());
    ExecutionPlanBuilder executionPlanBuilder = new ExecutionPlanBuilder(resources, network);
    ExecutionPlan plan = executionPlanBuilder.build(config, graph, taskSchedulePlan);
    Executor executor = new Executor(config, plan, network.getChannel());
    executor.execute();

    // we need to communicationProgress the channel
    while (true) {
      network.getChannel().progress();
    }
  }

  public WorkerPlan createWorkerPlan(AllocatedResources resourcePlan) {
    List<Worker> workers = new ArrayList<>();
    for (WorkerComputeResource resource : resourcePlan.getWorkerComputeResources()) {
      Worker w = new Worker(resource.getId());
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
    public boolean execute(IMessage message) {

      LOG.info("Message Partition Received : " + message.getContent()
          + ", Count : " + count);
      //hdfsConnector.HDFSConnect(message.getContent().toString());
      count++;
      return true;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void prepare(Config cfg, TaskContext context) {
      this.ctx = context;
      this.config = cfg;

      Map<String, Object> configs = context.getConfigurations();
      for (Map.Entry<String, Object> entry : configs.entrySet()) {
        if (entry.getKey().contains("outputdataset")) {
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
