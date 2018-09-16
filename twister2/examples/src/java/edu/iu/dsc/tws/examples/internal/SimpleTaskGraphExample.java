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
package edu.iu.dsc.tws.examples.internal;

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
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.discovery.IWorkerController;
import edu.iu.dsc.tws.common.resource.AllocatedResources;
import edu.iu.dsc.tws.common.resource.WorkerComputeResource;
import edu.iu.dsc.tws.common.worker.IPersistentVolume;
import edu.iu.dsc.tws.common.worker.IVolatileVolume;
import edu.iu.dsc.tws.common.worker.IWorker;
import edu.iu.dsc.tws.executor.core.OperationNames;
import edu.iu.dsc.tws.rsched.core.ResourceAllocator;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
import edu.iu.dsc.tws.task.api.ICompute;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.TaskContext;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.GraphBuilder;
import edu.iu.dsc.tws.task.graph.GraphConstants;
import edu.iu.dsc.tws.task.graph.OperationMode;
import edu.iu.dsc.tws.tsched.spi.scheduler.Worker;
import edu.iu.dsc.tws.tsched.spi.scheduler.WorkerPlan;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedulePlan;
import edu.iu.dsc.tws.tsched.taskscheduler.TaskScheduler;

public class SimpleTaskGraphExample implements IWorker {

  private static final Logger LOG = Logger.getLogger(SimpleTaskGraphExample.class.getName());

  public static void main(String[] args) {
    // first load the configurations from command line and config files
    Config config = ResourceAllocator.loadConfig(new HashMap<>());

    // build JobConfig
    HashMap<String, Object> configurations = new HashMap<>();
    configurations.put(SchedulerContext.THREADS_PER_WORKER, 8);

    JobConfig jobConfig = new JobConfig();
    jobConfig.putAll(configurations);

    // build the job
    Twister2Job twister2Job = Twister2Job.newBuilder()
            .setName("basic-taskgraphJob")
            .setWorkerClass(SimpleTaskGraphExample.class.getName())
            .setRequestResource(new WorkerComputeResource(2, 1024, 1.0), 2)
            .setConfig(jobConfig)
            .build();

    // now submit the job
    Twister2Submitter.submitJob(twister2Job, config);
  }

  /**
   * This is the execute method for the task graph.
   * @param config
   * @param workerID
   * @param resources
   * @param workerController
   * @param persistentVolume
   * @param volatileVolume
   */
  public void execute(Config config, int workerID, AllocatedResources resources,
                      IWorkerController workerController,
                      IPersistentVolume persistentVolume,
                      IVolatileVolume volatileVolume) {

    LOG.log(Level.INFO, "Starting the example with container id: " + resources.getWorkerId());
    TaskMapper taskMapper = new TaskMapper("task1");
    TaskReducer taskReducer = new TaskReducer("task2");
    TaskShuffler taskShuffler = new TaskShuffler("task3");
    TaskMerger taskMerger = new TaskMerger("task4");

    GraphBuilder builder = GraphBuilder.newBuilder();
    builder.addTask("task1", taskMapper);
    builder.addTask("task2", taskReducer);
    builder.addTask("task3", taskShuffler);
    builder.addTask("task4", taskMerger);

    builder.connect("task1", "task2", "partition-edge1",
            OperationNames.PARTITION);
    builder.connect("task1", "task3", "partition-edge2",
            OperationNames.PARTITION);
    builder.connect("task2", "task4", "partition-edge3",
            OperationNames.PARTITION);
    builder.connect("task3", "task4", "partition-edge4",
            OperationNames.PARTITION);

    builder.operationMode(OperationMode.BATCH);

    builder.setParallelism("task1", 2);
    builder.setParallelism("task2", 2);
    builder.setParallelism("task3", 2);
    builder.setParallelism("task4", 2);

    builder.addConfiguration("task1", "Ram", GraphConstants.taskInstanceRam(config));
    builder.addConfiguration("task1", "Disk", GraphConstants.taskInstanceDisk(config));
    builder.addConfiguration("task1", "Cpu", GraphConstants.taskInstanceCpu(config));

    builder.addConfiguration("task2", "Ram", GraphConstants.taskInstanceRam(config));
    builder.addConfiguration("task2", "Disk", GraphConstants.taskInstanceDisk(config));
    builder.addConfiguration("task2", "Cpu", GraphConstants.taskInstanceCpu(config));

    builder.addConfiguration("task3", "Ram", GraphConstants.taskInstanceRam(config));
    builder.addConfiguration("task3", "Disk", GraphConstants.taskInstanceDisk(config));
    builder.addConfiguration("task3", "Cpu", GraphConstants.taskInstanceCpu(config));

    builder.addConfiguration("task4", "Ram", GraphConstants.taskInstanceRam(config));
    builder.addConfiguration("task4", "Disk", GraphConstants.taskInstanceDisk(config));
    builder.addConfiguration("task4", "Cpu", GraphConstants.taskInstanceCpu(config));

    List<String> sourceInputDataset = new ArrayList<>();
    sourceInputDataset.add("dataset1.txt");
    sourceInputDataset.add("dataset2.txt");

    builder.addConfiguration("task1", "inputdataset", sourceInputDataset);
    builder.addConfiguration("task2", "inputdataset", sourceInputDataset);
    builder.addConfiguration("task3", "inputdataset", sourceInputDataset);
    builder.addConfiguration("task4", "inputdataset", sourceInputDataset);

    DataFlowTaskGraph graph = builder.build();
    WorkerPlan workerPlan = createWorkerPlan(resources);

    LOG.info("Generated Dataflow Task Graph Is:" + graph.getTaskVertexSet());

    //For scheduling streaming task
    if (workerID == 0) {
      TaskScheduler taskScheduler = new TaskScheduler();
      taskScheduler.initialize(config);
      TaskSchedulePlan taskSchedulePlan = taskScheduler.schedule(graph, workerPlan);

      Map<Integer, TaskSchedulePlan.ContainerPlan> containersMap
              = taskSchedulePlan.getContainersMap();
      for (Map.Entry<Integer, TaskSchedulePlan.ContainerPlan> entry : containersMap.entrySet()) {
        Integer integer = entry.getKey();
        TaskSchedulePlan.ContainerPlan containerPlan = entry.getValue();
        Set<TaskSchedulePlan.TaskInstancePlan> taskInstancePlans
                = containerPlan.getTaskInstances();
        //int containerId = containerPlan.getRequiredResource().getId();
        LOG.info("Task Details for Container Id:" + integer);
        for (TaskSchedulePlan.TaskInstancePlan ip : taskInstancePlans) {
          LOG.info("Task Id:" + ip.getTaskId() + "\tTask Index" + ip.getTaskIndex()
                  + "\tTask Name:" + ip.getTaskName());
        }
      }
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

  private class TaskMapper implements ICompute {
    private static final long serialVersionUID = 3233011943332591934L;
    public String taskName = null;

    protected TaskMapper(String taskName1) {
      this.taskName = taskName1;
    }

    @Override
    public void prepare(Config cfg, TaskContext collection) {
    }

    @Override
    public boolean execute(IMessage content) {
      return true;
    }
  }

  private class TaskReducer implements ICompute {
    private static final long serialVersionUID = 3233011943332591934L;
    public String taskName = null;

    protected TaskReducer(String taskName1) {
      this.taskName = taskName1;
    }

    @Override
    public void prepare(Config cfg, TaskContext collection) {
    }

    @Override
    public boolean execute(IMessage content) {
      return true;
    }
  }

  private class TaskShuffler implements ICompute {
    private static final long serialVersionUID = 3233011943332591934L;
    public String taskName = null;

    protected TaskShuffler(String taskName1) {
      this.taskName = taskName1;
    }

    @Override
    public void prepare(Config cfg, TaskContext collection) {
    }

    @Override
    public boolean execute(IMessage content) {
      return true;
    }
  }

  private class TaskMerger implements ICompute {
    private static final long serialVersionUID = 3233011943332591934L;
    public String taskName = null;

    protected TaskMerger(String taskName1) {
      this.taskName = taskName1;
    }

    @Override
    public void prepare(Config cfg, TaskContext collection) {
    }

    @Override
    public boolean execute(IMessage content) {
      return true;
    }
  }
}



