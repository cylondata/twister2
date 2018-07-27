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
package edu.iu.dsc.tws.examples.task.batch;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Submitter;
import edu.iu.dsc.tws.api.basic.job.BasicJob;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageFlags;
import edu.iu.dsc.tws.comms.core.TWSNetwork;
import edu.iu.dsc.tws.executor.api.ExecutionModel;
import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.executor.comm.tasks.batch.SinkBatchTask;
import edu.iu.dsc.tws.executor.comm.tasks.batch.SourceBatchTask;
import edu.iu.dsc.tws.executor.core.CommunicationOperationType;
import edu.iu.dsc.tws.executor.core.ExecutionPlanBuilder;
import edu.iu.dsc.tws.executor.core.SinkTaskContextListener;
import edu.iu.dsc.tws.executor.core.SourceTaskContextListener;
import edu.iu.dsc.tws.executor.threading.ThreadExecutor;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
import edu.iu.dsc.tws.rsched.spi.container.IContainer;
import edu.iu.dsc.tws.rsched.spi.resource.ResourceContainer;
import edu.iu.dsc.tws.rsched.spi.resource.ResourcePlan;
import edu.iu.dsc.tws.task.api.IFunction;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.ITaskContext;
import edu.iu.dsc.tws.task.api.TaskContext;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.GraphBuilder;
import edu.iu.dsc.tws.task.graph.OperationMode;
import edu.iu.dsc.tws.tsched.roundrobin.RoundRobinTaskScheduling;
import edu.iu.dsc.tws.tsched.spi.scheduler.Worker;
import edu.iu.dsc.tws.tsched.spi.scheduler.WorkerPlan;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedulePlan;


public class ReduceBatchTask implements IContainer {
  @Override
  public void init(Config config, int id, ResourcePlan resourcePlan) {
    GeneratorTask g = new GeneratorTask();
    RecevingTask r = new RecevingTask();
    System.out.println("Reduce Batch Task Starting ...");
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

    RoundRobinTaskScheduling roundRobinTaskScheduling = new RoundRobinTaskScheduling();
    roundRobinTaskScheduling.initialize(config);

    WorkerPlan workerPlan = createWorkerPlan(resourcePlan);
    TaskSchedulePlan taskSchedulePlan = roundRobinTaskScheduling.schedule(graph, workerPlan);

    TWSNetwork network = new TWSNetwork(config, resourcePlan.getThisId());
    ExecutionPlanBuilder executionPlanBuilder = new ExecutionPlanBuilder(resourcePlan, network);
    ExecutionPlan plan = executionPlanBuilder.execute(config, graph, taskSchedulePlan);
    ExecutionModel executionModel = new ExecutionModel(ExecutionModel.SHARED);
    ThreadExecutor executor = new ThreadExecutor(executionModel, plan, network.getChannel());
    executor.execute();
  }

  private static class GeneratorTask extends SourceBatchTask implements ITaskContext {
    private static final long serialVersionUID = -254264903510284748L;
    private TaskContext sourceTaskContext;
    private Config config;
    private SourceTaskContextListener sourceTaskContextListener = null;
    private int count = 0;

    @Override
    public void run() {
      if (count == 0) {
        this.sourceTaskContext.write("reduce-edge", "Hello " + count);
      }

      if (count == 1) {
        this.sourceTaskContext.write("reduce-edge", MessageFlags.LAST_MESSAGE);
      }

      if (count > 10) {
        this.sourceTaskContext.setDone(true);
        this.sourceTaskContextListener.mutateContext(sourceTaskContext);
        this.setSourceTaskContextListener(this.sourceTaskContextListener);
      }
      count++;
    }

    @Override
    public void interrupt() {
      this.sourceTaskContextListener.onInterrupt();
    }

    @Override
    public void prepare(Config cfg, TaskContext context) {
      this.sourceTaskContext = context;
      this.sourceTaskContext.setDone(false);
      this.sourceTaskContextListener
          = new SourceTaskContextListener(this, sourceTaskContext);
      this.sourceTaskContextListener.mutateContext(sourceTaskContext);
      this.sourceTaskContextListener.onStart();
    }

    @Override
    public TaskContext getContext() {
      return this.sourceTaskContext;
    }

    @Override
    public void overrideTaskContext(TaskContext context) {
      this.sourceTaskContext = context;
    }

    @Override
    public SourceTaskContextListener getSourceTaskContextListener() {
      return this.sourceTaskContextListener;
    }

  }

  private static class RecevingTask extends SinkBatchTask implements ITaskContext {
    private static final long serialVersionUID = -254264903510284798L;
    private int count = 0;
    private Config config;
    private TaskContext sinkTaskContext;
    private SinkTaskContextListener sinkTaskContextListener = new SinkTaskContextListener();

    @Override
    public void execute(IMessage message) {
      if (count < 15) {
        System.out.println("Message Reduced : " + message.getContent() + ", Count : " + count);
      }
      count++;
    }

    @Override
    public void prepare(Config cfg, TaskContext context) {
      this.config = cfg;
      this.sinkTaskContext = context;
    }

    @Override
    public void overrideTaskContext(TaskContext context) {

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


  public WorkerPlan createWorkerPlan(ResourcePlan resourcePlan) {
    List<Worker> workers = new ArrayList<>();
    for (ResourceContainer resource : resourcePlan.getContainers()) {
      Worker w = new Worker(resource.getId());
      workers.add(w);
    }

    return new WorkerPlan(workers);
  }


  public static void main(String[] args) {
    // first load the configurations from command line and config files
    System.out.println("==================Reduce Batch Task Example========================");
    Config config = ResourceAllocator.loadConfig(new HashMap<>());
    HashMap<String, Object> configurations = new HashMap<>();
    configurations.put(SchedulerContext.THREADS_PER_WORKER, 1);

    // build JobConfig
    JobConfig jobConfig = new JobConfig();
    jobConfig.putAll(configurations);

    BasicJob.BasicJobBuilder jobBuilder = BasicJob.newBuilder();
    jobBuilder.setName("reduce-batch-task");
    jobBuilder.setContainerClass(ReduceBatchTask.class.getName());
    jobBuilder.setRequestResource(new ResourceContainer(1, 1024), 4);
    jobBuilder.setConfig(jobConfig);

    // now submit the job
    Twister2Submitter.submitContainerJob(jobBuilder.build(), config);
  }
}
