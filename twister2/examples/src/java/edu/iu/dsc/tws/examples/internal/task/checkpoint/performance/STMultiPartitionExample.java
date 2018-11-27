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
package edu.iu.dsc.tws.examples.internal.task.checkpoint.performance;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Submitter;
import edu.iu.dsc.tws.api.job.Twister2Job;
import edu.iu.dsc.tws.api.net.Network;
import edu.iu.dsc.tws.checkpointmanager.state_backend.FsCheckpointStorage;
import edu.iu.dsc.tws.checkpointmanager.utils.CheckpointContext;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.controller.IWorkerController;
import edu.iu.dsc.tws.common.worker.IPersistentVolume;
import edu.iu.dsc.tws.common.worker.IVolatileVolume;
import edu.iu.dsc.tws.common.worker.IWorker;
import edu.iu.dsc.tws.comms.api.TWSChannel;
import edu.iu.dsc.tws.comms.op.Communicator;
import edu.iu.dsc.tws.data.fs.Path;
import edu.iu.dsc.tws.data.fs.local.LocalFileSystem;
import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.executor.core.ExecutionPlanBuilder;
import edu.iu.dsc.tws.executor.core.Runtime;
import edu.iu.dsc.tws.executor.threading.Executor;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
import edu.iu.dsc.tws.task.api.ComputeCheckpointableTask;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.Operations;
import edu.iu.dsc.tws.task.api.SinkCheckpointableTask;
import edu.iu.dsc.tws.task.api.Snapshot;
import edu.iu.dsc.tws.task.api.SourceCheckpointableTask;
import edu.iu.dsc.tws.task.api.TaskContext;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.GraphBuilder;
import edu.iu.dsc.tws.task.graph.GraphConstants;
import edu.iu.dsc.tws.task.graph.OperationMode;
import edu.iu.dsc.tws.tsched.spi.scheduler.Worker;
import edu.iu.dsc.tws.tsched.spi.scheduler.WorkerPlan;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedulePlan;
import edu.iu.dsc.tws.tsched.streaming.roundrobin.RoundRobinTaskScheduler;

public class STMultiPartitionExample implements IWorker {

  private static final Logger LOG =
      Logger.getLogger(STMultiPartitionExample.class.getName());

  @Override
  public void execute(Config config, int workerID,
                      IWorkerController workerController,
                      IPersistentVolume persistentVolume,
                      IVolatileVolume volatileVolume) {

    Path path = new Path(new File(CheckpointContext
        .getStatebackendDirectoryDefault(config)).toURI());

    Runtime runtime = new Runtime();
    runtime.setParentpath(path);

    LocalFileSystem localFileSystem = new LocalFileSystem();
    runtime.setFileSystem(localFileSystem);
    Config newconfig = runtime.updateConfig(config);

    if (workerID == 1) {
      LOG.log(Level.INFO, "Statebackend directory is created for job: " + runtime.getJobName());
      FsCheckpointStorage newStateBackend = new FsCheckpointStorage(localFileSystem, path,
          runtime.getJobName(), 0);
    }

    GeneratorTask g = new GeneratorTask();

    MiddleTask m = new MiddleTask();

    ReceivingTask r = new ReceivingTask();

    GraphBuilder builder = GraphBuilder.newBuilder();

    builder.addSource("source", g);
    builder.setParallelism("source", 4);

    builder.addSink("sink", r);
    builder.setParallelism("sink", 4);

    builder.addTask("middle", m);
    builder.setParallelism("middle", 4);

    builder.connect("source", "middle", "partition-edge-1",
        Operations.PARTITION);
    builder.connect("middle", "sink", "partition-edge-2", Operations.PARTITION);
    builder.operationMode(OperationMode.STREAMING);

    builder.addConfiguration("source", "Ram", GraphConstants.taskInstanceRam(newconfig));
    builder.addConfiguration("source", "Disk", GraphConstants.taskInstanceDisk(newconfig));
    builder.addConfiguration("source", "Cpu", GraphConstants.taskInstanceCpu(newconfig));

    DataFlowTaskGraph graph = builder.build();

    RoundRobinTaskScheduler roundRobinTaskScheduler = new RoundRobinTaskScheduler();
    roundRobinTaskScheduler.initialize(newconfig);

    WorkerPlan workerPlan = createWorkerPlan(workerController.getAllWorkers());
    TaskSchedulePlan taskSchedulePlan = roundRobinTaskScheduler.schedule(graph, workerPlan);

    TWSChannel network = Network.initializeChannel(newconfig, workerController);
    ExecutionPlanBuilder executionPlanBuilder = new ExecutionPlanBuilder(workerID,
        workerController.getAllWorkers(),
        new Communicator(newconfig, network));
    ExecutionPlan plan = executionPlanBuilder.build(newconfig, graph, taskSchedulePlan);
    Executor executor = new Executor(newconfig, workerID, plan, network);
    executor.execute();
  }

  private static class GeneratorTask extends SourceCheckpointableTask {
    private static final long serialVersionUID = -254264903510284748L;
    private TaskContext ctx;
    private Config config;

    private int value = 0;
    private int myIndex;
    private int worldSize;
    private int limit = 1000000;

    @Override
    public void execute() {
      if (value == limit) {
        context.write("partition-edge-1", "end");
      } else if (value < limit) {
        context.write("partition-edge-1", value + myIndex * limit);
//      LOG.log(Level.INFO, "count for source " + this.context.taskId() + " is " + value);
        value++;
      }
    }

    @Override
    public void prepare(Config cfg, TaskContext context) {
      this.myIndex = cfg.getIntegerValue("twister2.container.id", 0);
      this.worldSize = context.getParallelism();
      super.prepare(cfg, context);
    }

    @Override
    public void restoreSnapshot(Snapshot snapshot) {
      super.restoreSnapshot(snapshot);
      value = (Integer) this.getState("value");
    }

    @Override
    public void addCheckpointableStates() {
      this.addState("value", value);
    }
  }


  public static class MiddleTask extends ComputeCheckpointableTask {

    private static final long serialVersionUID = -254264903231284798L;

    private int count = 0;

    @Override
    public void prepare(Config cfg, TaskContext ctx) {
      super.prepare(cfg, ctx);
    }

    @Override
    public boolean execute(IMessage content) {
      count++;
      if (count == 10000) {
        LOG.info("Count in middle task " + context.taskId()
            + " is " + count
            + " value is" + content
        );
      }
      context.write("partition-edge-2", content.toString());


      return true;
    }

    @Override
    public void restoreSnapshot(Snapshot newsnapshot) {
      super.restoreSnapshot(newsnapshot);
      count = (Integer) this.getState("count");
    }

    @Override
    public void addCheckpointableStates() {
      this.addState("count", count);
    }
  }

  private static class ReceivingTask extends SinkCheckpointableTask {
    private static final long serialVersionUID = -254264903511284798L;
    private Config config;

    private int count = 0;

    private TaskContext ctx;

    @Override
    public boolean execute(IMessage message) {
//      System.out.println(message.getContent() + " from Sink Task " + ctx.taskId());
      count++;
      LOG.log(Level.INFO, "count in sink " + ctx.taskId() + " is " + count);
      return true;
    }

    @Override
    public void prepare(Config cfg, TaskContext context) {
      this.ctx = context;
    }

    @Override
    public void restoreSnapshot(Snapshot snapshot) {
      super.restoreSnapshot(snapshot);
      count = (Integer) this.getState("count");
    }

    @Override
    public void addCheckpointableStates() {
      this.addState("count", count);
    }


  }

  public WorkerPlan createWorkerPlan(List<JobMasterAPI.WorkerInfo> workerInfoList) {
    List<Worker> workers = new ArrayList<>();
    for (JobMasterAPI.WorkerInfo workerInfo: workerInfoList) {
      Worker w = new Worker(workerInfo.getWorkerID());
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
    Twister2Job.Twister2JobBuilder jobBuilder = Twister2Job.newBuilder();
    jobBuilder.setJobName("st-multi-partition-checkpoint");
    jobBuilder.setWorkerClass(STMultiPartitionExample.class.getName());
    jobBuilder.addComputeResource(1, 512, 4);
    jobBuilder.setConfig(jobConfig);

    // now submit the job
    Twister2Submitter.submitJob(jobBuilder.build(), config);
  }
}




