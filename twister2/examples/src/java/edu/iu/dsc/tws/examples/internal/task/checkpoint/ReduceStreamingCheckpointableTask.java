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
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.discovery.IWorkerController;
import edu.iu.dsc.tws.common.resource.AllocatedResources;
import edu.iu.dsc.tws.common.resource.WorkerComputeResource;
import edu.iu.dsc.tws.common.worker.IPersistentVolume;
import edu.iu.dsc.tws.common.worker.IVolatileVolume;
import edu.iu.dsc.tws.common.worker.IWorker;
import edu.iu.dsc.tws.examples.internal.task.TaskUtils;
import edu.iu.dsc.tws.executor.core.OperationNames;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
import edu.iu.dsc.tws.task.api.IFunction;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.SinkCheckpointableTask;
import edu.iu.dsc.tws.task.api.SourceCheckpointableTask;
import edu.iu.dsc.tws.task.api.TaskContext;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.GraphBuilder;
import edu.iu.dsc.tws.task.graph.OperationMode;
import edu.iu.dsc.tws.tsched.spi.scheduler.Worker;
import edu.iu.dsc.tws.tsched.spi.scheduler.WorkerPlan;

public class ReduceStreamingCheckpointableTask implements IWorker {
  private static final Logger LOG
      = Logger.getLogger(ReduceStreamingCheckpointableTask.class.getName());

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
    builder.setParallelism("sink", 1);
    builder.connect("source", "sink", "reduce-edge",
        OperationNames.REDUCE, new IFunction() {
          @Override
          public Object onMessage(Object object1, Object object2) {
            return object1;
          }
        });
    builder.operationMode(OperationMode.STREAMING);
    DataFlowTaskGraph graph = builder.build();

    TaskUtils.execute(config, resources, graph, workerController);
  }

  private static class GeneratorTask extends SourceCheckpointableTask {
    private static final long serialVersionUID = -254264913510284748L;
    private int count;

    @Override
    public void prepare(Config cfg, TaskContext context) {
      connect(cfg, context);
      this.ctx = context;
    }

    @Override
    public void execute() {
      boolean wrote = ctx.write("reduce-edge", "Hello");
      if (wrote) {
        count++;
        if (count % 10000 == 0) {
          LOG.info(String.format("%d %d Reduce sent count : %d", ctx.getWorkerId(),
              ctx.taskId(), count));
        }
        if (count % 100000 == 0) {
          checkForBarrier();
        }
      }
    }
  }

  private static class ReceivingTask extends SinkCheckpointableTask {
    private static final long serialVersionUID = -254264903510184798L;
    private int count = 0;

    @Override
    public void prepare(Config cfg, TaskContext context) {
      this.ctx = context;

      connect(cfg, context);
    }

    @Override
    public boolean execute(IMessage message) {
      if (count % 10000 == 0) {
        LOG.info("Reduce receive count : " + count);
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
    HashMap<String, Object> configurations = new HashMap<>();
    configurations.put(SchedulerContext.THREADS_PER_WORKER, 8);

    // build JobConfig
    JobConfig jobConfig = new JobConfig();
    jobConfig.putAll(configurations);

    Twister2Job.BasicJobBuilder jobBuilder = Twister2Job.newBuilder();
    jobBuilder.setName("reduce-checkpointable-task");
    jobBuilder.setWorkerClass(ReduceStreamingCheckpointableTask.class.getName());
    jobBuilder.setRequestResource(new WorkerComputeResource(2, 1024), 4);
    jobBuilder.setConfig(jobConfig);

    // now submit the job
    Twister2Submitter.submitJob(jobBuilder.build(), config);
  }
}
