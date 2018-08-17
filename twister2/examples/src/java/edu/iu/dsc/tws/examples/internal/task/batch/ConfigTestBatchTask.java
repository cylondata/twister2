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
package edu.iu.dsc.tws.examples.internal.task.batch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Submitter;
import edu.iu.dsc.tws.api.job.Twister2Job;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.resource.WorkerComputeSpec;
import edu.iu.dsc.tws.common.resource.ZResourcePlan;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.core.TWSNetwork;
import edu.iu.dsc.tws.examples.internal.task.streaming.ConfigTestStreamingTask;
import edu.iu.dsc.tws.executor.api.ExecutionModel;
import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.executor.comm.tasks.batch.SinkBatchTask;
import edu.iu.dsc.tws.executor.comm.tasks.batch.SourceBatchTask;
import edu.iu.dsc.tws.executor.core.CommunicationOperationType;
import edu.iu.dsc.tws.executor.core.ExecutionPlanBuilder;
import edu.iu.dsc.tws.executor.threading.Executor;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
import edu.iu.dsc.tws.rsched.spi.container.IContainer;
import edu.iu.dsc.tws.task.api.IFunction;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.TaskContext;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.GraphBuilder;
import edu.iu.dsc.tws.task.graph.OperationMode;
import edu.iu.dsc.tws.tsched.spi.scheduler.Worker;
import edu.iu.dsc.tws.tsched.spi.scheduler.WorkerPlan;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedulePlan;
import edu.iu.dsc.tws.tsched.streaming.roundrobin.RoundRobinTaskScheduler;

public class ConfigTestBatchTask implements IContainer {
  public static final Logger LOG = Logger.getLogger(ConfigTestStreamingTask.class.getName());

  @Override
  public void init(Config config, int id, ZResourcePlan resourcePlan) {
    GeneratorTask g = new GeneratorTask();
    RecevingTask r = new RecevingTask();

    System.out.println("Config-Threads : " + SchedulerContext.numOfThreads(config));

    GraphBuilder builder = GraphBuilder.newBuilder();
    builder.addSource("source", g);
    builder.setParallelism("source", 4);
    builder.addSink("sink", r);
    builder.setParallelism("sink", 1);
    builder.connect("source", "sink", "reduce-edge",
        CommunicationOperationType.BATCH_REDUCE);
    builder.operationMode(OperationMode.BATCH);

    DataFlowTaskGraph graph = builder.build();

    RoundRobinTaskScheduler roundRobinTaskScheduler = new RoundRobinTaskScheduler();
    roundRobinTaskScheduler.initialize(config);

    WorkerPlan workerPlan = createWorkerPlan(resourcePlan);
    TaskSchedulePlan taskSchedulePlan = roundRobinTaskScheduler.schedule(graph, workerPlan);

    TWSNetwork network = new TWSNetwork(config, resourcePlan.getThisId());
    ExecutionPlanBuilder executionPlanBuilder = new ExecutionPlanBuilder(resourcePlan, network);
    ExecutionPlan plan = executionPlanBuilder.build(config, graph, taskSchedulePlan);
    ExecutionModel executionModel = new ExecutionModel(ExecutionModel.SHARING);
    Executor executor = new Executor(config, executionModel, plan, network.getChannel(),
        OperationMode.BATCH);
    executor.execute();
  }

  private static class GeneratorTask extends SourceBatchTask {
    private static final long serialVersionUID = -254264903510284748L;
    private TaskContext ctx;
    private Config config;

    @Override
    public void run() {
      ctx.write("reduce-edge", "Hello");
    }

    @Override
    public void interrupt() {

    }

    @Override
    public void prepare(Config cfg, TaskContext context) {
      this.ctx = context;
    }
  }

  private static class RecevingTask extends SinkBatchTask {
    private static final long serialVersionUID = -254264903510284798L;
    private int count = 0;

    @Override
    public boolean execute(IMessage message) {
      System.out.println("Message Reduced : " + message.getContent() + ", Count : " + count);
      count++;
      return true;
    }

    @Override
    public void prepare(Config cfg, TaskContext context) {

    }
  }

  public static class IdentityFunction implements IFunction {
    private static final long serialVersionUID = -254264903510284748L;

    @Override
    public void init(Config cfg, DataFlowOperation op, Map<Integer,
        List<Integer>> expectedIds, TaskContext context) {

    }

    @Override
    public boolean onMessage(int source, int path, int target, int flags, Object object) {
      System.out.println("Source : " + source + ", Path : " + path + "Target : " + target
          + " Object : " + object.getClass().getName());
      return true;
    }
  }


  public WorkerPlan createWorkerPlan(ZResourcePlan resourcePlan) {
    List<Worker> workers = new ArrayList<>();
    for (WorkerComputeSpec resource : resourcePlan.getContainers()) {
      Worker w = new Worker(resource.getId());
      workers.add(w);
    }

    return new WorkerPlan(workers);
  }

  public static void main(String[] args) {
    // first load the configurations from command line and config files
    LOG.info("Loading Configurations From ConfigTestStreamingTask");
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    HashMap<String, Object> configurations = new HashMap<>();
    configurations.put(SchedulerContext.THREADS_PER_WORKER, 8);

    // build JobConfig
    JobConfig jobConfig = new JobConfig();
    jobConfig.putAll(configurations);

    Twister2Job.BasicJobBuilder jobBuilder = Twister2Job.newBuilder();
    jobBuilder.setName("config-test-task");
    jobBuilder.setWorkerClass(ConfigTestBatchTask.class.getName());
    jobBuilder.setRequestResource(new WorkerComputeSpec(2, 1024), 4);
    jobBuilder.setConfig(jobConfig);

    // now submit the job
    LOG.info("Twister2Submitter In Action ... ");
    Twister2Submitter.submitContainerJob(jobBuilder.build(), config);
  }
}
