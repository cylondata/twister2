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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

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
import edu.iu.dsc.tws.executor.core.OperationNames;
import edu.iu.dsc.tws.executor.core.Runtime;
import edu.iu.dsc.tws.executor.threading.Executor;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.TaskContext;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.GraphBuilder;
import edu.iu.dsc.tws.task.graph.GraphConstants;
import edu.iu.dsc.tws.task.graph.OperationMode;
import edu.iu.dsc.tws.task.streaming.BaseStreamSink;
import edu.iu.dsc.tws.task.streaming.BaseStreamSource;
import edu.iu.dsc.tws.tsched.spi.scheduler.Worker;
import edu.iu.dsc.tws.tsched.spi.scheduler.WorkerPlan;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedulePlan;
import edu.iu.dsc.tws.tsched.streaming.roundrobin.RoundRobinTaskScheduler;


public class TaskStatebackendCountExample implements IWorker {
  @Override
  public void execute(Config config, int workerID,
                      IWorkerController workerController,
                      IPersistentVolume persistentVolume,
                      IVolatileVolume volatileVolume) {

    Path path = new Path(new File(CheckpointContext
        .getStatebackendDirectoryDefault(config)).toURI());
    path.getParent();

    Runtime runtime = new Runtime();
    runtime.setParentpath(path);

    LocalFileSystem localFileSystem = new LocalFileSystem();
    runtime.setFileSystem(localFileSystem);
    Config newconfig = runtime.updateConfig(config);

    if (CheckpointContext.getCheckpointRecovery(config)) {
      System.out.println("it is working okey");
    } else {
      System.out.println("don't worry be happy");
    }

    if (workerID == 1) {
      System.out.println("Statebackend directory is created for job: " + runtime.getJobName());
      FsCheckpointStorage newStateBackend = new FsCheckpointStorage(localFileSystem, path,
          runtime.getJobName(), 0);

    }

    GeneratorTask g = new GeneratorTask();
    ReceivingTask r = new ReceivingTask();
    GraphBuilder builder = GraphBuilder.newBuilder();
    builder.addSource("source", g);
    builder.setParallelism("source", 4);
    builder.addSink("sink", r);
    builder.setParallelism("sink", 4);
    builder.connect("source", "sink", "partition-edge",
        OperationNames.PARTITION);
    builder.operationMode(OperationMode.STREAMING);

    builder.addConfiguration("source", "Ram", GraphConstants.taskInstanceRam(newconfig));
    builder.addConfiguration("source", "Disk", GraphConstants.taskInstanceDisk(newconfig));
    builder.addConfiguration("source", "Cpu", GraphConstants.taskInstanceCpu(newconfig));

    List<String> sourceInputDataset = new ArrayList<>();
    sourceInputDataset.add("dataset1.txt");
    sourceInputDataset.add("dataset2.txt");

    builder.addConfiguration("source", "inputdataset", sourceInputDataset);

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

  private static class GeneratorTask extends BaseStreamSource {
    private static final long serialVersionUID = -254264903510284748L;
    private TaskContext ctx;
    private Config config;
    private int sleeper = 0;
    private int counter = 0;

    @Override
    public void execute() {
      sleeper++;
      if (sleeper % 1000 == 0) {
        counter++;
      }
      if (sleeper > 200000) {
        ctx.write("partition-edge", counter);
        sleeper = 0;
      }

    }

    @Override
    public void prepare(Config cfg, TaskContext context) {
      this.ctx = context;
    }
  }

  private static class ReceivingTask extends BaseStreamSink {
    private static final long serialVersionUID = -254264903510284798L;
    private int count = 0;

    @Override
    public boolean execute(IMessage message) {
      System.out.println("Count received is :" + message.getContent());
      return true;
    }

    @Override
    public void prepare(Config cfg, TaskContext context) {

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

  public static void main(String[] args) throws IOException {

    // first load the configurations from command line and config files
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    // build JobConfig
    HashMap<String, Object> configurations = new HashMap<>();
    configurations.put(SchedulerContext.THREADS_PER_WORKER, 8);

    // build JobConfig
    JobConfig jobConfig = new JobConfig();
    jobConfig.putAll(configurations);
    Twister2Job.Twister2JobBuilder jobBuilder = Twister2Job.newBuilder();
    jobBuilder.setJobName("task-statebackend-count-example");
    jobBuilder.setWorkerClass(TaskStatebackendCountExample.class.getName());
    jobBuilder.addComputeResource(2, 1024, 4);
    jobBuilder.setConfig(jobConfig);

    Twister2Submitter.submitJob(jobBuilder.build(), config);
  }
}


