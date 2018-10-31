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
package edu.iu.dsc.tws.examples.internal.task.checkpoint;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Submitter;
import edu.iu.dsc.tws.api.job.Twister2Job;
import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.api.task.TaskWorker;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.resource.AllocatedResources;
import edu.iu.dsc.tws.common.resource.WorkerComputeResource;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.examples.internal.task.TaskUtils;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.SinkCheckpointableTask;
import edu.iu.dsc.tws.task.api.SourceCheckpointableTask;
import edu.iu.dsc.tws.task.api.TaskContext;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.OperationMode;
import edu.iu.dsc.tws.task.streaming.BaseStreamCompute;
import edu.iu.dsc.tws.tsched.spi.scheduler.Worker;
import edu.iu.dsc.tws.tsched.spi.scheduler.WorkerPlan;


public class MultiPartitionCheckpointExample extends TaskWorker {
  private static final Logger LOG
      = Logger.getLogger(MultiPartitionCheckpointExample.class.getName());

  @Override
  public void execute() {
    GeneratorTask g = new GeneratorTask();
    SinkTask rt = new SinkTask();
    PartitionTask r = new PartitionTask();

    TaskGraphBuilder builder = TaskGraphBuilder.newBuilder(config);
    builder.addSource("source", g, 4);
    ComputeConnection pc = builder.addCompute("compute", r, 4);
    pc.partition("source", "partition-edge", DataType.OBJECT);
    ComputeConnection pc1 = builder.addSink("sink", rt, 1);
    pc1.partition("compute", "partition-sink-edge", DataType.OBJECT);

    builder.setMode(OperationMode.STREAMING);

    DataFlowTaskGraph graph = builder.build();
    TaskUtils.execute(config, allocatedResources, graph, workerController);
  }

  private static class GeneratorTask extends SourceCheckpointableTask {
    private static final long serialVersionUID = -254264903510284748L;

    private int count = 0;

    @Override
    public void addCheckpointableStates() {

    }

    @Override
    public void prepare(Config cfg, TaskContext ctx) {
      super.prepare(cfg, ctx);
      connect(cfg, context);
    }

    @Override
    public void execute() {
      if (count % 1000000 == 0) {
        checkForBarrier();
      }
      if (count % 1000000 == 0) {
        ctx.write("partition-edge", "Hello");
        System.out.println("count is " + count);
      }

      count++;
    }
  }

  private static class SinkTask extends SinkCheckpointableTask {
    private static final long serialVersionUID = -254264903510224791L;
    private int count = 0;

    @Override
    public void addCheckpointableStates() {

    }

    @Override
    public boolean execute(IMessage message) {
      if (message.getContent() instanceof List) {
        count += ((List) message.getContent()).size();
        for (Object o : (List) message.getContent()) {
          LOG.info(o.toString() + " from Sink Task " + ctx.taskId());
        }
      }
      return true;
    }

    @Override
    public void prepare(Config cfg, TaskContext ctx) {
      super.prepare(cfg, ctx);
    }
  }

  private static class PartitionTask extends BaseStreamCompute {
    private static final long serialVersionUID = -254264913910284798L;

    @Override
    public boolean execute(IMessage message) {
      if (message.getContent() instanceof List) {
        for (Object o : (List) message.getContent()) {
          context.write("partition-sink-edge", o);
        }
      }
      return true;
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
    jobBuilder.setName("multi-partition-checkpoint-example");
    jobBuilder.setWorkerClass(MultiPartitionCheckpointExample.class.getName());
    jobBuilder.setRequestResource(new WorkerComputeResource(1, 512), 4);
    jobBuilder.setConfig(jobConfig);

    // now submit the job
    Twister2Submitter.submitJob(jobBuilder.build(), config);
  }
}
