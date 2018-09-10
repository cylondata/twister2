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

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.JobConfig;
import edu.iu.dsc.tws.api.Twister2Submitter;
import edu.iu.dsc.tws.api.job.Twister2Job;
import edu.iu.dsc.tws.api.net.Network;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.discovery.IWorkerController;
import edu.iu.dsc.tws.common.resource.AllocatedResources;
import edu.iu.dsc.tws.common.resource.WorkerComputeResource;
import edu.iu.dsc.tws.common.worker.IPersistentVolume;
import edu.iu.dsc.tws.common.worker.IVolatileVolume;
import edu.iu.dsc.tws.common.worker.IWorker;
import edu.iu.dsc.tws.comms.api.TWSChannel;
import edu.iu.dsc.tws.comms.op.Communicator;
import edu.iu.dsc.tws.data.fs.Path;
import edu.iu.dsc.tws.data.hdfs.HadoopDataOutputStream;
import edu.iu.dsc.tws.data.hdfs.HadoopFileSystem;
import edu.iu.dsc.tws.data.utils.HdfsUtils;
import edu.iu.dsc.tws.executor.api.ExecutionPlan;
import edu.iu.dsc.tws.executor.core.ExecutionPlanBuilder;
import edu.iu.dsc.tws.executor.core.OperationNames;
import edu.iu.dsc.tws.executor.threading.Executor;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
import edu.iu.dsc.tws.task.api.ICompute;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.TaskContext;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.GraphBuilder;
import edu.iu.dsc.tws.task.graph.OperationMode;
import edu.iu.dsc.tws.tsched.spi.scheduler.Worker;
import edu.iu.dsc.tws.tsched.spi.scheduler.WorkerPlan;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedulePlan;
import edu.iu.dsc.tws.tsched.taskscheduler.TaskScheduler;

public class HDFSTaskExample implements IWorker {

  private static final Logger LOG = Logger.getLogger(HDFSTaskExample.class.getName());

  private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;

  private static HdfsUtils hdfsConnector;

  public static void main(String[] args) {

    // first load the configurations from command line and config files
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    // build JobConfig
    HashMap<String, Object> configurations = new HashMap<>();
    configurations.put(SchedulerContext.THREADS_PER_WORKER, 8);

    JobConfig jobConfig = new JobConfig();
    jobConfig.putAll(configurations);

    Twister2Job.BasicJobBuilder jobBuilder = Twister2Job.newBuilder();
    jobBuilder.setName("hdfstask-example");
    jobBuilder.setWorkerClass(HDFSTaskExample.class.getName());
    jobBuilder.setRequestResource(new WorkerComputeResource(2, 1024), 2);
    jobBuilder.setConfig(jobConfig);

    // now submit the job
    Twister2Submitter.submitJob(jobBuilder.build(), config);
  }

  /**
   * This method initialize the config, container id, and resource plan objects.
   */
  public void execute(Config config, int workerID, AllocatedResources resources,
                      IWorkerController workerController,
                      IPersistentVolume persistentVolume,
                      IVolatileVolume volatileVolume) {

    TaskMapper taskMapper = new TaskMapper();
    TaskReducer taskReducer = new TaskReducer();

    GraphBuilder builder = GraphBuilder.newBuilder();
    builder.addTask("task1", taskMapper);
    builder.addTask("task2", taskReducer);

    builder.setParallelism("task1", 2);
    builder.setParallelism("task2", 2);

    List<String> inputList = new ArrayList<>();
    inputList.add("dataset1.txt");

    builder.addConfiguration("task1", "inputdataset", inputList);
    builder.addConfiguration("task2", "inputdataset", inputList);

    List<String> outputList = new ArrayList<>();
    outputList.add("datasetout.txt");

    builder.addConfiguration("task1", "outputdataset", outputList);
    builder.addConfiguration("task2", "outputdataset", outputList);

    builder.connect("task1", "task2", OperationNames.PARTITION);
    builder.operationMode(OperationMode.STREAMING);

    WorkerPlan workerPlan = createWorkerPlan(resources);
    DataFlowTaskGraph graph = builder.build();

    TaskScheduler taskScheduler = new TaskScheduler();
    taskScheduler.initialize(config);
    TaskSchedulePlan taskSchedulePlan = taskScheduler.schedule(graph, workerPlan);

    //Just to print the task schedule plan...
    if (workerID == 0) {
      if (taskSchedulePlan != null) {
        Map<Integer, TaskSchedulePlan.ContainerPlan> containersMap
                = taskSchedulePlan.getContainersMap();
        for (Map.Entry<Integer, TaskSchedulePlan.ContainerPlan> entry : containersMap.entrySet()) {
          Integer integer = entry.getKey();
          TaskSchedulePlan.ContainerPlan containerPlan = entry.getValue();
          Set<TaskSchedulePlan.TaskInstancePlan> containerPlanTaskInstances
                  = containerPlan.getTaskInstances();
          LOG.info("Task Details for Container Id:" + integer);
          for (TaskSchedulePlan.TaskInstancePlan ip : containerPlanTaskInstances) {
            LOG.info("Task Id:" + ip.getTaskId()
                    + "\tTask Index" + ip.getTaskIndex()
                    + "\tTask Name:" + ip.getTaskName());
          }
        }
      }
    }

    TWSChannel network = Network.initializeChannel(config, workerController, resources);
    ExecutionPlanBuilder executionPlanBuilder = new ExecutionPlanBuilder(resources,
            new Communicator(config, network));
    ExecutionPlan plan = executionPlanBuilder.build(config, graph, taskSchedulePlan);
    Executor executor = new Executor(config, workerID, plan, network);
    //executor.execute();
  }

  public WorkerPlan createWorkerPlan(AllocatedResources resourcePlan) {
    List<Worker> workers = new ArrayList<>();
    for (WorkerComputeResource resource : resourcePlan.getWorkerComputeResources()) {
      Worker w = new Worker(resource.getId());
      workers.add(w);
    }
    return new WorkerPlan(workers);
  }


  private static class TaskMapper implements ICompute {
    private static final long serialVersionUID = 3233011943332591934L;
    public String taskName;
    private TaskContext ctx;
    private Config config;
    private String inputFileName;
    private String outputFileName;

    @SuppressWarnings("unchecked")
    @Override
    public void prepare(Config cfg, TaskContext context) {
      this.ctx = context;
      this.config = cfg;

      Map<String, Object> configs = ctx.getConfigurations();

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

    @Override
    public boolean execute(IMessage content) {
      hdfsConnector = new HdfsUtils(config, inputFileName);
      HadoopFileSystem hadoopFileSystem = hdfsConnector.createHDFSFileSystem();
      Path path = hdfsConnector.getPath();

      //Reading Input Files
      BufferedReader br = null;
      try {
        if (!hadoopFileSystem.exists(path)) {
          br = new BufferedReader(new InputStreamReader(hadoopFileSystem.open(path)));
          while (br.readLine() != null) {
            br.read();
          }
        } else {
          throw new FileNotFoundException("File Not Found In HDFS");
        }
      } catch (IOException e) {
        e.printStackTrace();
      } finally {
        try {
          br.close();
          hadoopFileSystem.close();
        } catch (IOException ioe) {
          ioe.printStackTrace();
        }
      }

      //Writing to Output Files
      hdfsConnector = new HdfsUtils(config, outputFileName);
      HadoopFileSystem hadoopfileSystem = hdfsConnector.createHDFSFileSystem();
      path = hdfsConnector.getPath();
      HadoopDataOutputStream hadoopDataOutputStream;
      try {
        if (!hadoopfileSystem.exists(path)) {
          hadoopDataOutputStream = hadoopFileSystem.create(path);
          for (int i = 0; i < 10; i++) {
            hadoopDataOutputStream.write(
                    "Hello, I am writing to Hadoop Data Output Stream\n".getBytes(DEFAULT_CHARSET));
          }
        }
      } catch (IOException ioe) {
        ioe.printStackTrace();
      }

      return true;
    }
  }


  private static class TaskReducer implements ICompute {
    private static final long serialVersionUID = 3233011943332591934L;
    private TaskContext ctx;
    private String outputFile;
    private Config config;
    private String taskName;

    @SuppressWarnings("unchecked")
    @Override
    public void prepare(Config cfg, TaskContext context) {
      this.ctx = context;
      this.config = cfg;

      Map<String, Object> configs = ctx.getConfigurations();
      for (Map.Entry<String, Object> entry : configs.entrySet()) {
        if (entry.getKey().contains("outputdataset")) {
          List<String> outputFiles = (List<String>) entry.getValue();
          for (int i = 0; i < outputFiles.size(); i++) {
            this.outputFile = outputFiles.get(i);
          }
        }
      }
    }

    @Override
    public boolean execute(IMessage content) {
      return true;
    }
  }
}
