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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Submitter;
import edu.iu.dsc.tws.api.basic.job.BasicJob;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.core.TWSNetwork;
import edu.iu.dsc.tws.executor.ExecutionPlan;
import edu.iu.dsc.tws.executor.ExecutionPlanBuilder;
import edu.iu.dsc.tws.executor.threading.ExecutionModel;
import edu.iu.dsc.tws.executor.threading.ThreadExecutor;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
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
import edu.iu.dsc.tws.tsched.datalocalityaware.DataLocalityAwareTaskScheduling;
import edu.iu.dsc.tws.tsched.roundrobin.RoundRobinTaskScheduling;
import edu.iu.dsc.tws.tsched.spi.common.TaskSchedulerContext;
import edu.iu.dsc.tws.tsched.spi.scheduler.Worker;
import edu.iu.dsc.tws.tsched.spi.scheduler.WorkerPlan;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedulePlan;

public class HDFSDataLocalityExecutorExample implements IContainer {
  public static void main(String[] args) {
    // first load the configurations from command line and config files
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    // build JobConfig
    JobConfig jobConfig = new JobConfig();

    BasicJob.BasicJobBuilder jobBuilder = BasicJob.newBuilder();
    jobBuilder.setName("hdfs-task-datalocality-example");
    jobBuilder.setContainerClass(HDFSDataLocalityExecutorExample.class.getName());
    jobBuilder.setRequestResource(new ResourceContainer(2, 1024), 2);
    jobBuilder.setConfig(jobConfig);

    // now submit the job
    Twister2Submitter.submitContainerJob(jobBuilder.build(), config);
  }

  @Override
  public void init(Config config, int id, ResourcePlan resourcePlan) {
    GeneratorTask g = new GeneratorTask();
    ReceivingTask r = new ReceivingTask();

    GraphBuilder builder = GraphBuilder.newBuilder();
    builder.addSource("source", g);
    builder.setParallelism("source", 2);
    builder.addSink("sink", r);
    builder.setParallelism("sink", 2);
    builder.connect("source", "sink", "partition-edge", Operations.PARTITION);

    //Adding source task property configurations

    List<String> sourceInputDataset = new ArrayList<>();
    sourceInputDataset.add("dataset1.txt");
    sourceInputDataset.add("dataset2.txt");

    builder.addConfiguration("source", "Ram", GraphConstants.taskInstanceRam(config));
    builder.addConfiguration("source", "Disk", GraphConstants.taskInstanceDisk(config));
    builder.addConfiguration("source", "Cpu", GraphConstants.taskInstanceCpu(config));
    builder.addConfiguration("source", "inputdataset", sourceInputDataset);

    //Adding sink task property configurations

    List<String> sinkInputDataset = new ArrayList<>();
    sinkInputDataset.add("dataset3.txt");
    sinkInputDataset.add("dataset4.txt");

    builder.addConfiguration("sink", "Ram", GraphConstants.taskInstanceRam(config));
    builder.addConfiguration("sink", "Disk", GraphConstants.taskInstanceDisk(config));
    builder.addConfiguration("sink", "Cpu", GraphConstants.taskInstanceCpu(config));
    builder.addConfiguration("sink", "inputdataset", sinkInputDataset);

    DataFlowTaskGraph graph = builder.build();

    TaskSchedulePlan taskSchedulePlan = null;
    WorkerPlan workerPlan = createWorkerPlan(resourcePlan);

    /*WorkerPlan workerPlan = new WorkerPlan();
    Worker worker0 = new Worker(0);
    Worker worker1 = new Worker(1);
    Worker worker2 = new Worker(2);

    worker0.setCpu(4);
    worker0.setDisk(4000);
    worker0.setRam(2048);
    worker0.addProperty("bandwidth", 1000.0);
    worker0.addProperty("latency", 0.1);

    worker1.setCpu(4);
    worker1.setDisk(4000);
    worker1.setRam(2048);
    worker1.addProperty("bandwidth", 2000.0);
    worker1.addProperty("latency", 0.1);

    worker2.setCpu(4);
    worker2.setDisk(4000);
    worker2.setRam(2048);
    worker2.addProperty("bandwidth", 3000.0);
    worker2.addProperty("latency", 0.1);

    workerPlan.addWorker(worker0);
    workerPlan.addWorker(worker1);
    workerPlan.addWorker(worker2);*/

    if (TaskSchedulerContext.taskSchedulingMode(config).equals("datalocalityaware")) {
      DataLocalityAwareTaskScheduling dataLocalityAwareTaskScheduling = new
          DataLocalityAwareTaskScheduling();
      dataLocalityAwareTaskScheduling.initialize(config);
      taskSchedulePlan = dataLocalityAwareTaskScheduling.schedule(graph, workerPlan);
    } else if (TaskSchedulerContext.taskSchedulingMode(config).equals("roundrobin")) {
      RoundRobinTaskScheduling roundRobinTaskScheduling = new RoundRobinTaskScheduling();
      roundRobinTaskScheduling.initialize(config);
      taskSchedulePlan = roundRobinTaskScheduling.schedule(graph, workerPlan);
    }

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
    private Config config;

    @Override
    public void run() {
      ctx.write("partition-edge", "Hello");
    }

    @Override
    public void prepare(Config cfg, TaskContext context) {
      this.ctx = context;
      java.util.Map<String, Object> configs = context.getConfigurations();
      for (java.util.Map.Entry<String, Object> entry : configs.entrySet()) {
        System.out.println("key: " + entry.getKey() + "; value: " + entry.getValue());
        if (entry.getKey().toString().contains("inputdataset")) {
          System.out.println("Required Key and Value:"
              + entry.getKey() + "\t" + entry.getValue());
        }
      }
    }
  }

  private static class ReceivingTask extends SinkTask {

    private static final long serialVersionUID = -254264903510284798L;
    private int count = 0;

    @Override
    public void execute(IMessage message) {
      System.out.println("Message Partition Received : " + message.getContent()
          + ", Count : " + count);
      count++;
    }

    @Override
    public void prepare(Config cfg, TaskContext context) {
    }
  }
}
