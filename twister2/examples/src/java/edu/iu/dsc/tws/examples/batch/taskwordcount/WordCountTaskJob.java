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
package edu.iu.dsc.tws.examples.batch.taskwordcount;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Submitter;
import edu.iu.dsc.tws.api.job.Twister2Job;
import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.api.task.TaskWorker;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.resource.AllocatedResources;
import edu.iu.dsc.tws.common.resource.WorkerComputeResource;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.examples.internal.task.batch.PartitionBatchTask;
import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.task.api.IFunction;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.batch.BaseBatchSink;
import edu.iu.dsc.tws.task.batch.BaseBatchSource;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.OperationMode;
import edu.iu.dsc.tws.tsched.spi.scheduler.Worker;
import edu.iu.dsc.tws.tsched.spi.scheduler.WorkerPlan;

public class WordCountTaskJob extends TaskWorker {
  @Override
  public void execute() {
    WordSource g = new WordSource();
    WordAggregator r = new WordAggregator();

    TaskGraphBuilder builder = TaskGraphBuilder.newBuilder(config);
    builder.addSource("word-source", g, 4);

    builder.addSink("word-aggregator", r, 4).keyedReduce("word-source", "reduce", new IFunction() {
      @Override
      public Object onMessage(Object object1, Object object2) {
        return null;
      }
    }, DataType.OBJECT, DataType.INTEGER);
    builder.setMode(OperationMode.BATCH);

    DataFlowTaskGraph graph = builder.build();
    ExecutionPlan plan = taskExecutor.plan(graph);
    // this is a blocking call
    taskExecutor.execute(graph, plan);
  }

  private static class WordSource extends BaseBatchSource {
    private static final long serialVersionUID = -254264903510284748L;

    private int count = 0;

    @Override
    public void execute() {
      if (count == 999) {
        if (context.writeEnd("partition-edge", "Hello")) {
          count++;
        }
      } else if (count < 999) {
        if (context.write("partition-edge", "Hello")) {
          count++;
        }
      }
    }
  }

  private static class WordAggregator extends BaseBatchSink {
    private static final long serialVersionUID = -254264903510284798L;
    private int count = 0;

    @Override
    public boolean execute(IMessage message) {
      if (message.getContent() instanceof Iterator) {
        while (((Iterator) message.getContent()).hasNext()) {
          ((Iterator) message.getContent()).next();
          count++;
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
    JobConfig jobConfig = new JobConfig();
    Twister2Job.BasicJobBuilder jobBuilder = Twister2Job.newBuilder();
    jobBuilder.setName("wordcount-task");
    jobBuilder.setWorkerClass(PartitionBatchTask.class.getName());
    jobBuilder.setRequestResource(new WorkerComputeResource(2, 1024), 4);
    jobBuilder.setConfig(jobConfig);

    // now submit the job
    Twister2Submitter.submitJob(jobBuilder.build(), config);
  }
}
