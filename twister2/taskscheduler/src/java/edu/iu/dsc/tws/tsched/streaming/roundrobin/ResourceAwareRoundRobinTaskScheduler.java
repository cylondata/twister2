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
package edu.iu.dsc.tws.tsched.streaming.roundrobin;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.Vertex;
import edu.iu.dsc.tws.tsched.builder.TaskSchedulePlanBuilder;
import edu.iu.dsc.tws.tsched.spi.common.TaskSchedulerContext;
import edu.iu.dsc.tws.tsched.spi.scheduler.TaskSchedulerException;
import edu.iu.dsc.tws.tsched.spi.scheduler.WorkerPlan;
import edu.iu.dsc.tws.tsched.spi.taskschedule.ITaskScheduler;
import edu.iu.dsc.tws.tsched.spi.taskschedule.Resource;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedulePlan;
import edu.iu.dsc.tws.tsched.utils.TaskAttributes;
import edu.iu.dsc.tws.tsched.utils.TaskScheduleUtils;

/**
 * This class allocate the task instances into the container in a round robin manner. First, it will
 * allocate the task instances into the logical container values and then it will calculate the
 * required ram, disk, and cpu values for task instances and the containers which is based
 * on the task configuration values and the allocated worker values respectively.
 * <p>
 * For example, if there are two tasks with parallelism value of 2, 1st task -> instance 0 will
 * go to container 0, 2nd task -> instance 0 will go to container 1, 1st task -> instance 1 will
 * go to container 1, 2nd task -> instance 1 will go to container 1.
 */
public class ResourceAwareRoundRobinTaskScheduler implements ITaskScheduler {

  private static final Logger LOG = Logger.getLogger(
      ResourceAwareRoundRobinTaskScheduler.class.getName());

  //Represents the task schedule plan Id
  private static int taskSchedulePlanId = 0;

  //Represents the task instance ram
  private Double instanceRAM;

  //Represents the task instance disk
  private Double instanceDisk;

  //Represents the task instance cpu value
  private Double instanceCPU;

  //Config object
  private Config config;

  //Number of containers
  private int numberOfContainers;

  private int containerId;

  private Resource defaultResourceValue;
  private Resource maxContainerResourceValue;

  private int paddingPercentage;

  private int defaultNoOfTaskInstances;

  private Set<Vertex> taskVertexSet = new HashSet<>();
  private TaskAttributes taskAttributes = new TaskAttributes();
  private WorkerPlan workerplan = new WorkerPlan();

  @Override
  public void initialize(Config cfg) {
    /*this.config = cfg;
    this.instanceRAM = TaskSchedulerContext.taskInstanceRam(config);
    this.instanceDisk = TaskSchedulerContext.taskInstanceDisk(config);
    this.instanceCPU = TaskSchedulerContext.taskInstanceCpu(config);

    this.defaultResourceValue = new Resource(instanceRAM, this.instanceDisk, this.instanceCPU);
    this.defaultNoOfTaskInstances = TaskSchedulerContext.defaultTaskInstancesPerContainer(
        this.config);*/

    this.config = cfg;

    this.instanceRAM = TaskSchedulerContext.taskInstanceRam(this.config);
    this.instanceDisk = TaskSchedulerContext.taskInstanceDisk(this.config);
    this.instanceCPU = TaskSchedulerContext.taskInstanceCpu(this.config);

    this.paddingPercentage = TaskSchedulerContext.containerPaddingPercentage(this.config);
    this.defaultResourceValue = new Resource(instanceRAM, this.instanceDisk, this.instanceCPU);
    this.defaultNoOfTaskInstances = TaskSchedulerContext.defaultTaskInstancesPerContainer(
        this.config);

    instanceRAM = this.defaultResourceValue.getRam() * defaultNoOfTaskInstances;
    instanceDisk = this.defaultResourceValue.getDisk() * defaultNoOfTaskInstances;
    instanceCPU = this.defaultResourceValue.getCpu() * defaultNoOfTaskInstances;

    this.maxContainerResourceValue = new Resource(
        (double) Math.round(TaskScheduleUtils.increaseBy(instanceRAM, 100)),
        (double) Math.round(TaskScheduleUtils.increaseBy(instanceDisk, 100)),
        (double) Math.round(TaskScheduleUtils.increaseBy(instanceCPU, 100)));

    LOG.info("Instance default values:" + "RamValue:" + instanceRAM + "\t"
        + "DiskValue:" + instanceDisk + "\t" + "CPUValue:" + instanceCPU);

    LOG.info("Container default values:"
        + "RamValue:" + this.maxContainerResourceValue.getRam() + "\t"
        + "DiskValue:" + this.maxContainerResourceValue.getDisk() + "\t"
        + "CPUValue:" + this.maxContainerResourceValue.getCpu());

    this.numberOfContainers = TaskSchedulerContext.taskParallelism(config);
  }

  /*** This method set the size of the container, instance default resource, container padding,
   * ram map, disk map, and cpu map values.
   * @param previousTaskPlan
   * @return
   */
  private TaskSchedulePlanBuilder TaskSchedulingPlanBuilder(TaskSchedulePlan previousTaskPlan) {
    return new TaskSchedulePlanBuilder(1, previousTaskPlan)
        .setContainerMaximumResourceValue(maxContainerResourceValue)
        .setInstanceDefaultResourceValue(defaultResourceValue)
        .setRequestedContainerPadding(paddingPercentage)
        .setTaskRamMap(taskAttributes.getTaskRamMap(this.taskVertexSet))
        .setTaskDiskMap(taskAttributes.getTaskDiskMap(this.taskVertexSet))
        .setTaskCpuMap(taskAttributes.getTaskCPUMap(this.taskVertexSet));
  }

  @Override
  public TaskSchedulePlan schedule(DataFlowTaskGraph dataFlowTaskGraph, WorkerPlan workerPlan) {

    this.taskVertexSet = dataFlowTaskGraph.getTaskVertexSet();

    TaskSchedulePlanBuilder taskSchedulePlanBuilder = null;
    try {
      taskSchedulePlanBuilder = TaskSchedulingPlanBuilder(null);
      taskSchedulePlanBuilder.updateNumContainers(numberOfContainers);
      taskSchedulePlanBuilder = resourceAwareTaskSchedule(taskSchedulePlanBuilder);
      //return taskSchedulePlanBuilder.build();
    } catch (TaskSchedulerException tse) {
      LOG.info(String.format("%s Increase the number of containers %s",
          tse.getMessage(), this.numberOfContainers + 1));
      increaseNumberOfContainers(1);
      resetToFirstContainer();
    }
    return taskSchedulePlanBuilder.build();
  }

  private void increaseNumberOfContainers(int additionalContainers) {
    this.numberOfContainers += additionalContainers;
  }

  private void resetToFirstContainer() {
    this.containerId = 1;
  }

  private TaskSchedulePlanBuilder resourceAwareTaskSchedule(
      TaskSchedulePlanBuilder taskSchedulePlanBuilder) throws TaskSchedulerException {

    Map<String, Integer> parallelTaskMap = taskAttributes.getParallelTaskMap(this.taskVertexSet);
    int totalTaskInstances = taskAttributes.getTotalNumberOfInstances(this.taskVertexSet);

    LOG.info("Number of containers:" + numberOfContainers
        + "\tTotal Task Instances:" + totalTaskInstances);

    if (numberOfContainers > totalTaskInstances) {
      LOG.warning(String.format(
          "More containers requested (%s) than total instances (%s). Reducing containers to %s",
          numberOfContainers, totalTaskInstances, totalTaskInstances));
      numberOfContainers = totalTaskInstances;
      taskSchedulePlanBuilder.updateNumContainers(totalTaskInstances);
    }
    assignInstancesToContainers(taskSchedulePlanBuilder, parallelTaskMap);
    return taskSchedulePlanBuilder;
  }

  private void assignInstancesToContainers(TaskSchedulePlanBuilder taskSchedulePlanBuilder,
                                           Map<String, Integer> parallelTaskMap)
                                           throws TaskSchedulerException {
    for (String taskName : parallelTaskMap.keySet()) {
      int numberOfInstances = parallelTaskMap.get(taskName);
      for (int j = 0; j < numberOfInstances; j++) {
        assignTaskInstances(taskSchedulePlanBuilder, taskName);
      }
    }
  }

  private void assignTaskInstances(TaskSchedulePlanBuilder taskSchedulePlanBuilder,
                                   String taskName) throws TaskSchedulerException {
    LOG.info("Container Id:" + this.containerId);
    taskSchedulePlanBuilder.addInstance(this.containerId, taskName);
    this.containerId = nextContainerId(this.containerId);
  }

  private int nextContainerId(int afterId) {
    return (afterId == numberOfContainers) ? 1 : afterId + 1;
  }
}
