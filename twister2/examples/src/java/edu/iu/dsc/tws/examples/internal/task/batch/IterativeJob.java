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

import java.util.HashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Submitter;
import edu.iu.dsc.tws.api.job.Twister2Job;
import edu.iu.dsc.tws.api.task.TaskWorker;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.resource.WorkerComputeResource;
import edu.iu.dsc.tws.executor.core.CommunicationOperationType;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.ISink;
import edu.iu.dsc.tws.task.api.ISource;
import edu.iu.dsc.tws.task.batch.BaseBatchSinkTask;
import edu.iu.dsc.tws.task.batch.BaseBatchSourceTask;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.GraphBuilder;
import edu.iu.dsc.tws.task.graph.OperationMode;

public class IterativeJob extends TaskWorker {
  private static final Logger LOG = Logger.getLogger(IterativeJob.class.getName());

  @Override
  public void execute() {
    ISource source = new IterativeSourceTask();
    ISink sink = new ReceiveTask();

    GraphBuilder builder = GraphBuilder.newBuilder();
    builder.addSource("source", source);
    builder.setParallelism("source", 4);
    builder.addSink("sink", sink);
    builder.setParallelism("sink", 1);
    builder.connect("source", "sink", "gather-edge",
        CommunicationOperationType.BATCH_GATHER);
    builder.operationMode(OperationMode.BATCH);

    DataFlowTaskGraph graph = builder.build();

    // this is a blocking call
    taskExecutor.execute(graph);
  }

  private static class IterativeSourceTask extends BaseBatchSourceTask {
    protected static final long serialVersionUID = -254264120110286748L;

    @Override
    public void run() {
      ctx.write("", "Hello");
    }
  }

  private static class ReceiveTask extends BaseBatchSinkTask {
    protected static final long serialVersionUID = -254264120110286748L;
    @Override
    public boolean execute(IMessage message) {
      LOG.log(Level.INFO, "Received message: " + message.getContent());
      return false;
    }
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
    jobBuilder.setName("iterative-job");
    jobBuilder.setWorkerClass(IterativeJob.class.getName());
    jobBuilder.setRequestResource(new WorkerComputeResource(4, 1024), 4);
    jobBuilder.setConfig(jobConfig);

    // now submit the job
    Twister2Submitter.submitJob(jobBuilder.build(), config);
  }
}
