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
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Submitter;
import edu.iu.dsc.tws.api.job.Twister2Job;
import edu.iu.dsc.tws.api.task.Collector;
import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.api.task.Receptor;
import edu.iu.dsc.tws.api.task.TaskGraphBuilder;
import edu.iu.dsc.tws.api.task.TaskWorker;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.resource.WorkerComputeResource;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.dataset.DataSet;
import edu.iu.dsc.tws.dataset.Partition;
import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.batch.BaseBatchSink;
import edu.iu.dsc.tws.task.batch.BaseBatchSource;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.OperationMode;

public class IterativeJob extends TaskWorker {
  private static final Logger LOG = Logger.getLogger(IterativeJob.class.getName());

  @Override
  public void execute() {
    LOG.log(Level.INFO, "Task worker starting: " + workerId);

    IterativeSourceTask g = new IterativeSourceTask();
    PartitionTask r = new PartitionTask();

    TaskGraphBuilder graphBuilder = TaskGraphBuilder.newBuilder(config);
    graphBuilder.addSource("source", g, 4);
    ComputeConnection computeConnection = graphBuilder.addSink("sink", r, 4);
    computeConnection.partition("source", "partition", DataType.OBJECT);
    graphBuilder.setMode(OperationMode.BATCH);

    DataFlowTaskGraph graph = graphBuilder.build();
    for (int i = 0; i < 10; i++) {
      ExecutionPlan plan = taskExecutor.plan(graph);
      taskExecutor.addInput(graph, plan, "source", "input", new DataSet<>(0));

      // this is a blocking call
      taskExecutor.execute(graph, plan);
      DataSet<Object> dataSet = taskExecutor.getOutput(graph, plan, "sink");
      Set<Object> values = dataSet.getData();
      LOG.log(Level.INFO, "Values: " + values);
    }
  }

  private static class IterativeSourceTask extends BaseBatchSource implements Receptor {
    private static final long serialVersionUID = -254264120110286748L;

    private DataSet<Object> input;

    private int count = 0;

    @Override
    public void execute() {
      if (count == 999) {
        if (context.writeEnd("partition", "Hello")) {
          count++;
        }
      } else if (count < 999) {
        if (context.write("partition", "Hello")) {
          count++;
        }
      }
    }

    @Override
    public void add(String name, DataSet<Object> data) {
      LOG.log(Level.INFO, "Received input: " + name);
      input = data;
    }
  }

  private static class PartitionTask extends BaseBatchSink implements Collector<Object> {
    private static final long serialVersionUID = -5190777711234234L;

    private List<String> list = new ArrayList<>();

    private int count;

    @Override
    public boolean execute(IMessage message) {
      LOG.log(Level.INFO, "Received message: " + message.getContent());

      if (message.getContent() instanceof Iterator) {
        while (((Iterator) message.getContent()).hasNext()) {
          Object ret = ((Iterator) message.getContent()).next();
          count++;
          list.add(ret.toString());
        }
        LOG.info("Message Partition Received : " + message.getContent() + ", Count : " + count);
      }
      count++;
      return true;
    }

    @Override
    public Partition<Object> get() {
      return new Partition<>(context.taskIndex(), list);
    }
  }

  public static void main(String[] args) {
    LOG.log(Level.INFO, "Iterative job");
    // first load the configurations from command line and config files
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    // build JobConfig
    HashMap<String, Object> configurations = new HashMap<>();
    configurations.put(SchedulerContext.THREADS_PER_WORKER, 8);

    // build JobConfig
    JobConfig jobConfig = new JobConfig();
    jobConfig.putAll(configurations);

    Twister2Job.BasicJobBuilder jobBuilder = Twister2Job.newBuilder();
    jobBuilder.setName("iterative-job");
    jobBuilder.setWorkerClass(IterativeJob.class.getName());
    jobBuilder.setRequestResource(new WorkerComputeResource(4, 1024), 4);
    jobBuilder.setConfig(jobConfig);

    // now submit the job
    Twister2Submitter.submitJob(jobBuilder.build(), config);
  }
}
