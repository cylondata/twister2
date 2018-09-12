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

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Submitter;
import edu.iu.dsc.tws.api.job.Twister2Job;
import edu.iu.dsc.tws.api.task.TaskWorker;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.resource.AllocatedResources;
import edu.iu.dsc.tws.common.resource.WorkerComputeResource;
import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.executor.core.OperationNames;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.batch.BaseBatchSink;
import edu.iu.dsc.tws.task.batch.BaseBatchSource;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.GraphBuilder;
import edu.iu.dsc.tws.task.graph.OperationMode;
import edu.iu.dsc.tws.tsched.spi.scheduler.Worker;
import edu.iu.dsc.tws.tsched.spi.scheduler.WorkerPlan;

public class PartitionKeyedBatchTask extends TaskWorker {
  @Override
  public void execute() {
    GeneratorTask g = new GeneratorTask();
    RecevingTask r = new RecevingTask();

    GraphBuilder builder = GraphBuilder.newBuilder();
    builder.addSource("source", g);
    builder.setParallelism("source", 4);
    builder.addSink("sink", r);
    builder.setParallelism("sink", 4);
    builder.connect("source", "sink", "partition-keyed-edge",
        OperationNames.KEYED_PARTITION);
    builder.operationMode(OperationMode.BATCH);

    DataFlowTaskGraph graph = builder.build();
    ExecutionPlan plan = taskExecutor.plan(graph);
    // this is a blocking call
    taskExecutor.execute(graph, plan);
  }

  private static class GeneratorTask extends BaseBatchSource {
    private static final long serialVersionUID = -254264903510284798L;

    @Override
    public void execute() {
      context.write("partition-keyed-edge", "Hello");
    }
  }

  private static class RecevingTask extends BaseBatchSink {
    private static final long serialVersionUID = -254264903510284798L;
    private int count = 0;

    @Override
    public boolean execute(IMessage message) {
      if (count % 1000000 == 0) {
        System.out.println("Message Partition Keyed:  : " + message.getContent()
            + ", Count : " + count);
      }
      count++;
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
    jobBuilder.setName("partition-keyed-example");
    jobBuilder.setWorkerClass(PartitionKeyedBatchTask.class.getName());
    jobBuilder.setRequestResource(new WorkerComputeResource(2, 1024), 4);
    jobBuilder.setConfig(jobConfig);

    // now submit the job
    Twister2Submitter.submitJob(jobBuilder.build(), config);
  }
}
