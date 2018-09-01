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
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageFlags;
import edu.iu.dsc.tws.examples.internal.task.TaskUtils;
import edu.iu.dsc.tws.executor.core.CommunicationOperationType;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
import edu.iu.dsc.tws.task.api.IFunction;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.TaskContext;
import edu.iu.dsc.tws.task.batch.BaseBatchSinkTask;
import edu.iu.dsc.tws.task.batch.BaseBatchSourceTask;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.GraphBuilder;
import edu.iu.dsc.tws.task.graph.OperationMode;
import edu.iu.dsc.tws.tsched.spi.scheduler.Worker;
import edu.iu.dsc.tws.tsched.spi.scheduler.WorkerPlan;

public class ReduceBatchTask implements IWorker {
  @Override
  public void execute(Config config, int workerID, AllocatedResources resources,
                      IWorkerController workerController,
                      IPersistentVolume persistentVolume,
                      IVolatileVolume volatileVolume) {
    GeneratorTask g = new GeneratorTask();
    RecevingTask r = new RecevingTask();

    GraphBuilder builder = GraphBuilder.newBuilder();
    builder.addSource("source", g);
    builder.setParallelism("source", 4);
    builder.addSink("sink", r);
    builder.setParallelism("sink", 1);
    builder.connect("source", "sink", "reduce-edge",
        CommunicationOperationType.BATCH_REDUCE);
    builder.operationMode(OperationMode.BATCH);

    DataFlowTaskGraph graph = builder.build();
    TaskUtils.executeBatch(config, resources, graph);
  }

  private static class GeneratorTask extends BaseBatchSourceTask {
    private static final long serialVersionUID = -254264903510284748L;
    private TaskContext sourceTaskContext;
    private Config config;
    private int count = 0;

    @Override
    public void run() {
      if (count == 0) {
        this.sourceTaskContext.write("reduce-edge", "Hello " + count);
      }

      if (count == 1) {
        //add a writeLast method rather than write
        this.sourceTaskContext.write("reduce-edge", MessageFlags.LAST_MESSAGE);
      }

      count++;
    }

    @Override
    public void prepare(Config cfg, TaskContext context) {
      this.sourceTaskContext = context;
      this.config = cfg;
    }

    public TaskContext getContext() {
      return this.sourceTaskContext;
    }
  }

  private static class RecevingTask extends BaseBatchSinkTask {
    private static final long serialVersionUID = -254264903510284798L;
    private int count = 0;
    private Config config;
    private TaskContext sinkTaskContext;

    @Override
    public boolean execute(IMessage message) {
      System.out.println("Message Reduced : " + message.getContent() + ", Count : " + count);
      boolean status = false;
      count++;
      status = count == 1;
      return status;
    }

    @Override
    public void prepare(Config cfg, TaskContext context) {
      this.config = cfg;
      this.sinkTaskContext = context;
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
    System.out.println("==================Reduce Batch Task Example========================");
    Config config = ResourceAllocator.loadConfig(new HashMap<>());
    HashMap<String, Object> configurations = new HashMap<>();
    configurations.put(SchedulerContext.THREADS_PER_WORKER, 1);

    // build JobConfig
    JobConfig jobConfig = new JobConfig();
    jobConfig.putAll(configurations);

    Twister2Job.BasicJobBuilder jobBuilder = Twister2Job.newBuilder();
    jobBuilder.setName("reduce-batch-task");
    jobBuilder.setWorkerClass(ReduceBatchTask.class.getName());
    jobBuilder.setRequestResource(new WorkerComputeResource(1, 1024), 4);
    jobBuilder.setConfig(jobConfig);

    // now submit the job
    Twister2Submitter.submitJob(jobBuilder.build(), config);
  }
}
