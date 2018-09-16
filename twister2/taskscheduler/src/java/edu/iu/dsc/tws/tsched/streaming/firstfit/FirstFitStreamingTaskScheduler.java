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

/**
 * This class allocate the task instances into the container in a heuristic manner. It first sort
 * the task instances based on the required ram configuration. Also, it provides the support of
 * heterogeneous container and task instance allocation.
 * <p>
 * For example, if there are two tasks with parallelism value of 2, 1st task -> instance 0 will
 * go to container 0, 1st task -> instance 1 will go to container 0, 2nd task -> instance 0 will
 * go to container 0 (if the total task instance required values doesn't reach the maximum size of
 * container 0. If the container has reached its maximum limit then it will allocate the
 * 2nd task -> instance 1 will go to container 1.
 */
public class FirstFitStreamingTaskScheduler implements ITaskScheduler {

  private static final Logger LOG = Logger.getLogger(
                                                    FirstFitStreamingTaskScheduler.class.getName());

  private Resource defaultResourceValue;
  private Resource maxContainerResourceValue;

  private int paddingPercentage;
  private int numContainers;
  private int defaultNoOfTaskInstances;

  private Double instanceRAM;
  private Double instanceDisk;
  private Double instanceCPU;
  private Config config;

  private Set<Vertex> taskVertexSet = new HashSet<>();
  private TaskAttributes taskAttributes = new TaskAttributes();
  private WorkerPlan workerplan = new WorkerPlan();

  /**
   * This method initialize the config values received from the user and assign the default
   * task instance value and container instance values.
   */
  @Override
  public void initialize(Config cfg) {

    this.config = cfg;
    this.instanceRAM = TaskSchedulerContext.taskInstanceRam(this.config);
    this.instanceDisk = TaskSchedulerContext.taskInstanceDisk(this.config);
    this.instanceCPU = TaskSchedulerContext.taskInstanceCpu(this.config);
    this.paddingPercentage = TaskSchedulerContext.containerPaddingPercentage(this.config);
    this.defaultResourceValue = new Resource(instanceRAM, this.instanceDisk, this.instanceCPU);
    this.defaultNoOfTaskInstances = TaskSchedulerContext.defaultTaskInstancesPerContainer(
        this.config);

    instanceRAM = this.defaultResourceValue.getRam() * defaultNoOfTaskInstances;
    this.instanceDisk = this.defaultResourceValue.getDisk() * defaultNoOfTaskInstances;
    this.instanceCPU = this.defaultResourceValue.getCpu() * defaultNoOfTaskInstances;

    this.maxContainerResourceValue = new Resource(
        (double) Math.round(TaskScheduleUtils.increaseBy(instanceRAM, paddingPercentage)),
        (double) Math.round(TaskScheduleUtils.increaseBy(instanceDisk, paddingPercentage)),
        (double) Math.round(TaskScheduleUtils.increaseBy(instanceCPU, paddingPercentage)));

    LOG.fine("Instance default values:" + "RamValue:" + instanceRAM + "\t"
        + "DiskValue:" + instanceDisk + "\t" + "CPUValue:" + instanceCPU);

    LOG.fine("Container default values:"
        + "RamValue:" + this.maxContainerResourceValue.getRam() + "\t"
        + "DiskValue:" + this.maxContainerResourceValue.getDisk() + "\t"
        + "CPUValue:" + this.maxContainerResourceValue.getCpu());
  }

  /**
   * This method set the size of the container, instance default resource, container padding,
   * ram map, disk map, and cpu map values.
   * @param previousTaskPlan
   * @return
   */
  private TaskSchedulePlanBuilder newTaskSchedulingPlanBuilder(TaskSchedulePlan previousTaskPlan) {
    return new TaskSchedulePlanBuilder(1, previousTaskPlan)
        .setContainerMaximumResourceValue(maxContainerResourceValue)
        .setInstanceDefaultResourceValue(defaultResourceValue)
        .setRequestedContainerPadding(paddingPercentage)
        .setTaskRamMap(taskAttributes.getTaskRamMap(this.taskVertexSet))
        .setTaskDiskMap(taskAttributes.getTaskDiskMap(this.taskVertexSet))
        .setTaskCpuMap(taskAttributes.getTaskCPUMap(this.taskVertexSet));
  }

  /**
   * This is the base method for the first fit task scheduling. It invokes the taskscheduleplan
   * builder to allocate the task instances into the containers.
   * @param dataFlowTaskGraph
   * @param workerPlan
   * @return
   */
  @Override
  public TaskSchedulePlan schedule(DataFlowTaskGraph dataFlowTaskGraph, WorkerPlan workerPlan) {
    this.taskVertexSet = dataFlowTaskGraph.getTaskVertexSet();
    this.workerplan = workerPlan;
    TaskSchedulePlanBuilder taskSchedulePlanBuilder = newTaskSchedulingPlanBuilder(null);
    try {
      taskSchedulePlanBuilder = FirstFitFTaskSchedulingAlgorithm(taskSchedulePlanBuilder);
    } catch (TaskSchedulerException te) {
      throw new TaskSchedulerException("Couldn't allocate all instances to task schedule plan", te);
    }
    return taskSchedulePlanBuilder.build();
  }

  /**
   * This method is internal to this task scheduling algorithm which retrieves the parallel task map
   * for the task vertex set and send the taskschedule plan builder object (with assigned default
   * values) and parallel task map for the task instances allocation.
   * @param taskSchedulePlanBuilder
   * @return
   * @throws TaskSchedulerException
   */
  private TaskSchedulePlanBuilder FirstFitFTaskSchedulingAlgorithm(
      TaskSchedulePlanBuilder taskSchedulePlanBuilder) throws TaskSchedulerException {
    Map<String, Integer> parallelTaskMap = taskAttributes.getParallelTaskMap(this.taskVertexSet);
    assignInstancesToContainers(taskSchedulePlanBuilder, parallelTaskMap);
    return taskSchedulePlanBuilder;
  }

  /**
   * This method retrieve the sorted array list of ram requirements map and allocate the task
   * instances available in the map.
   * @param taskSchedulePlanBuilder
   * @param parallelTaskMap
   * @throws TaskSchedulerException
   */
  private void assignInstancesToContainers(TaskSchedulePlanBuilder taskSchedulePlanBuilder,
                                           Map<String, Integer> parallelTaskMap)
                                           throws TaskSchedulerException {

    ArrayList<RequiredRam> ramRequirements = getSortedRAMInstances(parallelTaskMap.keySet());

    for (RequiredRam ramRequirement : ramRequirements) {
      String taskName = ramRequirement.getTaskName();
      int numberOfInstances = parallelTaskMap.get(taskName);
      for (int j = 0; j < numberOfInstances; j++) {
        firstFitInstanceAllocation(taskSchedulePlanBuilder, taskName);
      }
    }
  }

  /**
   * This method sort the task instances in an increasing order based on the required ram
   * configuration values.
   * @param taskNameSet
   * @return
   */
  private ArrayList<RequiredRam> getSortedRAMInstances(Set<String> taskNameSet) {

    ArrayList<RequiredRam> ramRequirements = new ArrayList<>();
    Map<String, Double> taskRamMap = taskAttributes.getTaskRamMap(this.taskVertexSet);

    for (String taskName : taskNameSet) {
      Resource resource = TaskScheduleUtils.getResourceRequirement(
          taskName, taskRamMap, this.defaultResourceValue,
          this.maxContainerResourceValue, this.paddingPercentage);
      ramRequirements.add(new RequiredRam(taskName, resource.getRam()));
    }

    ramRequirements.sort(Collections.reverseOrder());
    return ramRequirements;
  }

  /**
   * This method first increment the number of container if the value is zero. Then, it allocates
   * the task instances into the container which is based on the container Id score value.
   * @param taskSchedulePlanBuilder
   * @param taskName
   * @throws TaskSchedulerException
   */
  private void firstFitInstanceAllocation(TaskSchedulePlanBuilder taskSchedulePlanBuilder,
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




