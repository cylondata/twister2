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
package edu.iu.dsc.tws.tsched.streaming.firstfit;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.Vertex;
import edu.iu.dsc.tws.tsched.builder.ContainerIdScorer;
import edu.iu.dsc.tws.tsched.builder.TaskSchedulePlanBuilder;
import edu.iu.dsc.tws.tsched.spi.common.TaskSchedulerContext;
import edu.iu.dsc.tws.tsched.spi.scheduler.TaskSchedulerException;
import edu.iu.dsc.tws.tsched.spi.scheduler.WorkerPlan;
import edu.iu.dsc.tws.tsched.spi.taskschedule.ITaskScheduler;
import edu.iu.dsc.tws.tsched.spi.taskschedule.Resource;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedulePlan;
import edu.iu.dsc.tws.tsched.utils.RequiredRam;
import edu.iu.dsc.tws.tsched.utils.TaskAttributes;
import edu.iu.dsc.tws.tsched.utils.TaskScheduleUtils;

public class FirstFitStreamingTaskScheduler implements ITaskScheduler {

  private static final Logger LOG
      = Logger.getLogger(FirstFitStreamingTaskScheduler.class.getName());

  private Resource defaultResourceValue;
  private Resource maximumContainerResourceValue;
  private int paddingPercentage;
  private int numContainers;

  private Double instanceDisk;
  private Double instanceCPU;
  private Config cfg;

  //Newly added
  private Set<Vertex> taskVertexSet = new HashSet<>();
  private TaskAttributes taskAttributes = new TaskAttributes();
  private WorkerPlan workerplan = new WorkerPlan();

  /**
   * This method initialize the config values received from the user and set
   * the default instance value and container maximum value.
   */
  @Override
  public void initialize(Config config) {

    this.cfg = config;

    Double instanceRAM = TaskSchedulerContext.taskInstanceRam(cfg);
    this.instanceDisk = TaskSchedulerContext.taskInstanceDisk(cfg);
    this.instanceCPU = TaskSchedulerContext.taskInstanceCpu(cfg);
    this.paddingPercentage = TaskSchedulerContext.containerPaddingPercentage(cfg);

    this.defaultResourceValue = new Resource(instanceRAM, this.instanceDisk, this.instanceCPU);

    int defaultNoOfTaskInstances = TaskSchedulerContext.defaultTaskInstancesPerContainer(cfg);

    instanceRAM = this.defaultResourceValue.getRam() * defaultNoOfTaskInstances;
    this.instanceDisk = this.defaultResourceValue.getDisk() * defaultNoOfTaskInstances;
    this.instanceCPU = this.defaultResourceValue.getCpu() * defaultNoOfTaskInstances;

    /*Worker worker = workerplan.getWorker(0);
    this.maximumContainerResourceValue = new Resource(
        (double) Math.round(TaskScheduleUtils.increaseBy(worker.getRam(), paddingPercentage)),
        (double) Math.round(TaskScheduleUtils.increaseBy(worker.getCpu(), paddingPercentage)),
        (double) Math.round(TaskScheduleUtils.increaseBy(worker.getCpu(), paddingPercentage)));*/

    this.maximumContainerResourceValue = new Resource(
        (double) Math.round(TaskScheduleUtils.increaseBy(instanceRAM, paddingPercentage)),
        (double) Math.round(TaskScheduleUtils.increaseBy(instanceDisk, paddingPercentage)),
        (double) Math.round(TaskScheduleUtils.increaseBy(instanceCPU, paddingPercentage)));

    LOG.info("Instance default values:" + "RamValue:" + instanceRAM + "\t"
        + "DiskValue:" + instanceDisk + "\t" + "CPUValue:" + instanceCPU);

    LOG.info("Container default values:"
        + "RamValue:" + this.maximumContainerResourceValue.getRam() + "\t"
        + "DiskValue:" + this.maximumContainerResourceValue.getDisk() + "\t"
        + "CPUValue:" + this.maximumContainerResourceValue.getCpu());
  }

  private TaskSchedulePlanBuilder newTaskSchedulingPlanBuilder(TaskSchedulePlan previousTaskPlan) {
    return new TaskSchedulePlanBuilder(1, previousTaskPlan) //Get the proper id
        .setContainerMaximumResourceValue(maximumContainerResourceValue)
        .setInstanceDefaultResourceValue(defaultResourceValue)
        .setRequestedContainerPadding(paddingPercentage)
        .setTaskRamMap(taskAttributes.getTaskRamMap(this.taskVertexSet))
        .setTaskDiskMap(taskAttributes.getTaskDiskMap(this.taskVertexSet))
        .setTaskCpuMap(taskAttributes.getTaskCPUMap(this.taskVertexSet));
  }

  @Override
  public TaskSchedulePlan schedule(DataFlowTaskGraph dataFlowTaskGraph, WorkerPlan workerPlan) {
    this.taskVertexSet = dataFlowTaskGraph.getTaskVertexSet();
    this.workerplan = workerPlan;
    TaskSchedulePlanBuilder taskSchedulePlanBuilder = newTaskSchedulingPlanBuilder(null);
    try {
      taskSchedulePlanBuilder = FirstFitFTaskSchedulingAlgorithm(taskSchedulePlanBuilder);
    } catch (TaskSchedulerException te) {
      throw new TaskSchedulerException(
          "Couldn't allocate all instances to task schedule plan", te);
    }
    return taskSchedulePlanBuilder.build();
  }

  private TaskSchedulePlanBuilder FirstFitFTaskSchedulingAlgorithm(
      TaskSchedulePlanBuilder taskSchedulePlanBuilder) throws TaskSchedulerException {
    Map<String, Integer> parallelTaskMap = taskAttributes.getParallelTaskMap(this.taskVertexSet);
    assignInstancesToContainers(taskSchedulePlanBuilder, parallelTaskMap);
    return taskSchedulePlanBuilder;
  }

  private void assignInstancesToContainers(TaskSchedulePlanBuilder taskSchedulePlanBuilder,
                                           Map<String, Integer> parallelTaskMap)
      throws TaskSchedulerException {
    ArrayList<RequiredRam> ramRequirements = getSortedRAMInstances(parallelTaskMap.keySet());
    for (RequiredRam ramRequirement : ramRequirements) {
      String taskName = ramRequirement.getTaskName();
      int numberOfInstances = parallelTaskMap.get(taskName);
      LOG.info("Number of Instances Required For the Task Name:\t"
          + taskName + "\t" + numberOfInstances + "\n");
      for (int j = 0; j < numberOfInstances; j++) {
        FirstFitInstanceAllocation(taskSchedulePlanBuilder, taskName);
      }
    }
  }

  private ArrayList<RequiredRam> getSortedRAMInstances(Set<String> taskNameSet) {
    ArrayList<RequiredRam> ramRequirements = new ArrayList<>();
    Map<String, Double> taskRamMap = taskAttributes.getTaskRamMap(this.taskVertexSet);
    for (String taskName : taskNameSet) {
      Resource resource = TaskScheduleUtils.getResourceRequirement(
          taskName, taskRamMap, this.defaultResourceValue,
          this.maximumContainerResourceValue, this.paddingPercentage);
      ramRequirements.add(new RequiredRam(taskName, resource.getRam()));
    }
    ramRequirements.sort(Collections.reverseOrder());
    return ramRequirements;
  }

  private void FirstFitInstanceAllocation(TaskSchedulePlanBuilder taskSchedulePlanBuilder,
                                          String taskName) throws TaskSchedulerException {
    if (this.numContainers == 0) {
      taskSchedulePlanBuilder.updateNumContainers(++numContainers);
    }
    try {
      //taskSchedulePlanBuilder.addInstance(taskName);
      taskSchedulePlanBuilder.addInstance(new ContainerIdScorer(), taskName);
    } catch (TaskSchedulerException e) {
      taskSchedulePlanBuilder.updateNumContainers(++numContainers);
      taskSchedulePlanBuilder.addInstance(numContainers, taskName);
    }
  }
}




