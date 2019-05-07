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
package edu.iu.dsc.tws.examples.internal.hdfs;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Submitter;
import edu.iu.dsc.tws.api.job.Twister2Job;
import edu.iu.dsc.tws.api.net.Network;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.controller.IWorkerController;
import edu.iu.dsc.tws.common.exceptions.TimeoutException;
import edu.iu.dsc.tws.common.worker.IPersistentVolume;
import edu.iu.dsc.tws.common.worker.IVolatileVolume;
import edu.iu.dsc.tws.common.worker.IWorker;
import edu.iu.dsc.tws.comms.api.Communicator;
import edu.iu.dsc.tws.comms.api.TWSChannel;
import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.executor.core.ExecutionPlanBuilder;
import edu.iu.dsc.tws.executor.core.OperationNames;
import edu.iu.dsc.tws.executor.threading.Executor;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
import edu.iu.dsc.tws.task.api.BaseSink;
import edu.iu.dsc.tws.task.api.BaseSource;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.TaskContext;
import edu.iu.dsc.tws.task.api.schedule.ContainerPlan;
import edu.iu.dsc.tws.task.api.schedule.TaskInstancePlan;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.GraphBuilder;
import edu.iu.dsc.tws.task.graph.OperationMode;
import edu.iu.dsc.tws.tsched.spi.scheduler.Worker;
import edu.iu.dsc.tws.tsched.spi.scheduler.WorkerPlan;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedulePlan;
import edu.iu.dsc.tws.tsched.taskscheduler.TaskScheduler;

public class HDFSTaskExample implements IWorker {

  private static final Logger LOG = Logger.getLogger(HDFSTaskExample.class.getName());

  public static void main(String[] args) {

    // first load the configurations from command line and config files
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    // build JobConfig
    HashMap<String, Object> configurations = new HashMap<>();
    configurations.put(SchedulerContext.THREADS_PER_WORKER, 8);

    JobConfig jobConfig = new JobConfig();
    jobConfig.putAll(configurations);

    Twister2Job.Twister2JobBuilder jobBuilder = Twister2Job.newBuilder();
    jobBuilder.setJobName("hdfstask-example");
    jobBuilder.setWorkerClass(HDFSTaskExample.class.getName());
    jobBuilder.addComputeResource(1, 512, 2);
    jobBuilder.setConfig(jobConfig);

    // now submit the job
    Twister2Submitter.submitJob(jobBuilder.build(), config);
  }

  /**
   * This method initialize the config, container id, and resource plan objects.
   */
  public void execute(Config config, int workerID,
                      IWorkerController workerController,
                      IPersistentVolume persistentVolume,
                      IVolatileVolume volatileVolume) {

    GeneratorTask g = new GeneratorTask();
    ReceivingTask r = new ReceivingTask();

    GraphBuilder builder = GraphBuilder.newBuilder();

    builder.addSource("source", g);
    builder.setParallelism("source", 2);

    builder.addSink("sink", r);
    builder.setParallelism("sink", 2);

    builder.connect("source", "sink", "partition-edge",
        OperationNames.PARTITION);
    builder.operationMode(OperationMode.STREAMING);

    List<String> inputList = new ArrayList<>();
    inputList.add("dataset1.txt");

    builder.addConfiguration("source", "inputdataset", inputList);
    builder.addConfiguration("sink", "inputdataset", inputList);

    List<String> outputList = new ArrayList<>();
    outputList.add("datasetout.txt");

    builder.addConfiguration("source", "outputdataset", outputList);
    builder.addConfiguration("sink", "outputdataset", outputList);

    List<JobMasterAPI.WorkerInfo> workerList = null;
    try {
      workerList = workerController.getAllWorkers();
    } catch (TimeoutException timeoutException) {
      LOG.log(Level.SEVERE, timeoutException.getMessage(), timeoutException);
      return;
    }
    WorkerPlan workerPlan = createWorkerPlan(workerList);
    DataFlowTaskGraph graph = builder.build();

    TaskSchedulePlan taskSchedulePlan;

    TaskScheduler taskScheduler = new TaskScheduler();
    taskScheduler.initialize(config);
    taskSchedulePlan = taskScheduler.schedule(graph, workerPlan);

    //Just to print the task schedule plan...
    if (workerID == 0) {
      if (taskSchedulePlan != null) {
        Map<Integer, ContainerPlan> containersMap
            = taskSchedulePlan.getContainersMap();
        for (Map.Entry<Integer, ContainerPlan> entry : containersMap.entrySet()) {
          Integer integer = entry.getKey();
          ContainerPlan containerPlan = entry.getValue();
          Set<TaskInstancePlan> containerPlanTaskInstances
              = containerPlan.getTaskInstances();
          LOG.info("Task Details for Container Id:" + integer);
          for (TaskInstancePlan ip : containerPlanTaskInstances) {
            LOG.info("Task Id:" + ip.getTaskId()
                + "\tTask Index" + ip.getTaskIndex()
                + "\tTask Name:" + ip.getTaskName());
          }
        }
      }
    }

    TWSChannel network = Network.initializeChannel(config, workerController);
    ExecutionPlanBuilder executionPlanBuilder = new ExecutionPlanBuilder(workerID,
        workerList, new Communicator(config, network));
    ExecutionPlan plan = executionPlanBuilder.build(config, graph, taskSchedulePlan);
    Executor executor = new Executor(config, workerID, network);
    executor.execute(plan);
  }

  public WorkerPlan createWorkerPlan(List<JobMasterAPI.WorkerInfo> workerInfoList) {
    List<Worker> workers = new ArrayList<>();
    for (JobMasterAPI.WorkerInfo workerInfo : workerInfoList) {
      Worker w = new Worker(workerInfo.getWorkerID());
      workers.add(w);
    }

    return new WorkerPlan(workers);
  }

  private static class GeneratorTask extends BaseSource {

    private static final long serialVersionUID = -254264903510284748L;
    private int count = 0;
    private String inputFileName;
    private String outputFileName;

    @SuppressWarnings("unchecked")
    @Override
    public void prepare(Config cfg, TaskContext ctx) {
      this.context = ctx;
      this.config = cfg;

      Map<String, Object> configs = context.getConfigurations();
      for (Map.Entry<String, Object> entry : configs.entrySet()) {
        if (entry.getKey().contains("inputdataset")) {
          List<String> inputFiles = (List<String>) entry.getValue();
          if (inputFiles.size() == 1) {
            this.inputFileName = inputFiles.get(0);
          } else {
            for (int i = 0; i < inputFiles.size(); i++) {
              this.inputFileName = inputFiles.get(i);
            }
          }
        }
      }
    }

    @SuppressWarnings("unchecked")
    @Override
    public void execute() {
      if (count == 0) {
        HDFSReaderWriter hdfsReaderWriter = new HDFSReaderWriter(config, inputFileName);
        hdfsReaderWriter.readInputFromHDFS();
      }

      boolean wrote = context.write("partition-edge", "Hello");
      if (wrote) {
        count++;
        if (count % 100 == 0) {
          LOG.info(String.format("%d %d Message Partition sent count : %d", context.getWorkerId(),
              context.globalTaskId(), count));
        }
      }
    }
  }

  private static class ReceivingTask extends BaseSink {

    private static final long serialVersionUID = -254264903510284798L;
    private int count = 0;
    private String outputFileName;

    @SuppressWarnings("unchecked")
    @Override
    public void prepare(Config cfg, TaskContext ctx) {
      this.context = ctx;
      this.config = cfg;

      Map<String, Object> configs = context.getConfigurations();
      for (Map.Entry<String, Object> entry : configs.entrySet()) {
        if (entry.getKey().contains("outputdataset")) {
          List<String> outputFiles = (List<String>) entry.getValue();
          if (outputFiles.size() == 1) {
            this.outputFileName = outputFiles.get(0);
          } else {
            for (int i = 0; i < outputFiles.size(); i++) {
              this.outputFileName = outputFiles.get(i);
            }
          }
        }
      }

    }

    @SuppressWarnings("unchecked")
    @Override
    public boolean execute(IMessage message) {

      if (count == 0) {
        HDFSReaderWriter hdfsReaderWriter = new HDFSReaderWriter(config, outputFileName);
        hdfsReaderWriter.writeOutputToHDFS();
      }

      if (message.getContent() instanceof List) {
        count += ((List) message.getContent()).size();
      }
      LOG.info(String.format("%d %d Message Partition Received count: %d", context.getWorkerId(),
          context.globalTaskId(), count));
      return true;
    }
  }
}

