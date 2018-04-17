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
package edu.iu.dsc.tws.tsched.FirstFit;

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
import edu.iu.dsc.tws.tsched.spi.common.Context;
import edu.iu.dsc.tws.tsched.spi.common.TaskConfig;
import edu.iu.dsc.tws.tsched.spi.scheduler.TaskSchedulerException;
import edu.iu.dsc.tws.tsched.spi.scheduler.WorkerPlan;
import edu.iu.dsc.tws.tsched.spi.taskschedule.Resource;
import edu.iu.dsc.tws.tsched.spi.taskschedule.ScheduleException;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedule;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedulePlan;
import edu.iu.dsc.tws.tsched.utils.Job;
import edu.iu.dsc.tws.tsched.utils.JobAttributes;
import edu.iu.dsc.tws.tsched.utils.RequiredRam;
import edu.iu.dsc.tws.tsched.utils.TaskAttributes;
import edu.iu.dsc.tws.tsched.utils.TaskScheduleUtils;


public class FirstFitTaskScheduling implements TaskSchedule {

  private static final Logger LOG = Logger.getLogger(FirstFitTaskScheduling.class.getName());

  //These values should be replaced with an appropriate values...
  private static final double DEFAULT_DISK_PADDING_PER_CONTAINER = 12;
  private static final double DEFAULT_CPU_PADDING_PER_CONTAINER = 1;
  private static final double MIN_RAM_PER_INSTANCE = 180;
  private static final double DEFAULT_RAM_PADDING_PER_CONTAINER = 2;
  private static final double NOT_SPECIFIED_NUMBER_VALUE = -1;
  private static final int DEFAULT_CONTAINER_PADDING_PERCENTAGE = 1;
  private static final int DEFAULT_NUMBER_INSTANCES_PER_CONTAINER = 4;
  /////////////////////////////////////////////////////////////////

  private Job job;
  private TaskConfig config;
  private Resource defaultResourceValue;
  private Resource maximumContainerResourceValue;
  private int paddingPercentage;
  private int numContainers;

  private Double instanceRAM;
  private Double instanceDisk;
  private Double instanceCPU;
  private Config cfg;

  //Newly added
  private Set<Vertex> taskVertexSet = new HashSet<>();
  private TaskAttributes taskAttributes = new TaskAttributes();

  @Override
  public void initialize(TaskConfig configValue, Job jobObject) {
    this.config = configValue;
    this.job = jobObject;

    this.defaultResourceValue = new Resource(Context.instanceRam(config),
        Context.instanceDisk(config), Context.instanceCPU(config));
    this.paddingPercentage = JobAttributes.JOB_CONTAINER_PADDING_PERCENTAGE;

    instanceRAM = this.defaultResourceValue.getRam() * DEFAULT_NUMBER_INSTANCES_PER_CONTAINER;
    instanceDisk = this.defaultResourceValue.getDisk() * DEFAULT_NUMBER_INSTANCES_PER_CONTAINER;
    instanceCPU = this.defaultResourceValue.getCpu() * DEFAULT_NUMBER_INSTANCES_PER_CONTAINER;

    double jobContainerMaxRamValue = JobAttributes.JOB_CONTAINER_MAX_RAM_VALUE;
    double jobContainerMaxDiskValue = JobAttributes.JOB_CONTAINER_MAX_DISK_VALUE;
    double jobContainerMaxCpuValue = JobAttributes.JOB_CONTAINER_MAX_CPU_VALUE;

    //this.maximumContainerResourceValue = new Resource(instanceRAM, instanceDisk, instanceCPU);

    this.maximumContainerResourceValue = new Resource(
        (double) Math.round(TaskScheduleUtils.increaseBy(instanceRAM, paddingPercentage)),
        (double) Math.round(TaskScheduleUtils.increaseBy(instanceDisk, paddingPercentage)),
        (double) Math.round(TaskScheduleUtils.increaseBy(instanceCPU, paddingPercentage)));
  }

  @Override
  public void initialize(Job jobObject) {
    //check this place...
    TaskConfig configVal = new TaskConfig();

    this.config = configVal;
    this.job = jobObject;
    this.defaultResourceValue = new Resource(Context.instanceRam(config),
        Context.instanceDisk(config), Context.instanceCPU(config));
    this.paddingPercentage = JobAttributes.JOB_CONTAINER_PADDING_PERCENTAGE;

    this.instanceRAM = this.defaultResourceValue.getRam()
        * DEFAULT_NUMBER_INSTANCES_PER_CONTAINER;
    this.instanceDisk = this.defaultResourceValue.getDisk()
        * DEFAULT_NUMBER_INSTANCES_PER_CONTAINER;
    this.instanceCPU = this.defaultResourceValue.getCpu()
        * DEFAULT_NUMBER_INSTANCES_PER_CONTAINER;

    this.maximumContainerResourceValue = new Resource(instanceRAM, instanceDisk, instanceCPU);

    LOG.info("Instance default values:" + "RamValue:" + instanceRAM + "\t"
        + "DiskValue:" + instanceDisk + "\t" + "CPUValue:" + instanceCPU);

    LOG.info("Container maximum values:" + "RamValue:"
        + this.maximumContainerResourceValue.getRam() + "\t"
        + "DiskValue:" + this.maximumContainerResourceValue.getDisk() + "\t"
        + "CPUValue:" + this.maximumContainerResourceValue.getCpu());
  }


  public void initialize(Config cfg1) {
    this.cfg = cfg1;

    //Retrieve the default instance values from the config file.
    /*this.instanceRAM = Double.parseDouble(cfg.getStringValue("INSTANCE_RAM"))
        * DEFAULT_NUMBER_INSTANCES_PER_CONTAINER;
    this.instanceDisk = Double.parseDouble(cfg.getStringValue("INSTANCE_DISK"))
        * DEFAULT_NUMBER_INSTANCES_PER_CONTAINER;
    this.instanceCPU = Double.parseDouble(cfg.getStringValue("INSTANCE_CPU"))
        * DEFAULT_NUMBER_INSTANCES_PER_CONTAINER;*/

    this.instanceRAM = 4096.0;
    this.instanceDisk = 2000.0;
    this.instanceCPU = 10.0;

    LOG.info("Instance default values:" + "RamValue:" + instanceRAM + "\t"
        + "DiskValue:" + instanceDisk + "\t" + "CPUValue:" + instanceCPU);

    this.defaultResourceValue = new Resource(this.instanceRAM, this.instanceDisk, this.instanceCPU);

    this.instanceRAM = this.defaultResourceValue.getRam()
        * DEFAULT_NUMBER_INSTANCES_PER_CONTAINER;
    this.instanceDisk = this.defaultResourceValue.getDisk()
        * DEFAULT_NUMBER_INSTANCES_PER_CONTAINER;
    this.instanceCPU = this.defaultResourceValue.getCpu()
        * DEFAULT_NUMBER_INSTANCES_PER_CONTAINER;

    //this.maximumContainerResourceValue = new Resource(instanceRAM, instanceDisk, instanceCPU);

    this.paddingPercentage = DEFAULT_CONTAINER_PADDING_PERCENTAGE;

    this.maximumContainerResourceValue = new Resource(
        (double) Math.round(TaskScheduleUtils.increaseBy(instanceRAM, paddingPercentage)),
        (double) Math.round(TaskScheduleUtils.increaseBy(instanceDisk, paddingPercentage)),
        (double) Math.round(TaskScheduleUtils.increaseBy(instanceCPU, paddingPercentage)));

    LOG.info("Container default values:"
        + "RamValue:" + this.maximumContainerResourceValue.getRam() + "\t"
        + "DiskValue:" + this.maximumContainerResourceValue.getDisk() + "\t"
        + "CPUValue:" + this.maximumContainerResourceValue.getCpu());
  }

  private TaskSchedulePlanBuilder newTaskSchedulingPlanBuilder(TaskSchedulePlan previousTaskPlan) {
    //return new TaskSchedulePlanBuilder(job.getJobId(), previousTaskPlan)
    return new TaskSchedulePlanBuilder(1, previousTaskPlan) //Get the proper id
        .setContainerMaximumResourceValue(maximumContainerResourceValue)
        .setInstanceDefaultResourceValue(defaultResourceValue)
        .setRequestedContainerPadding(paddingPercentage)
        .setTaskRamMap(taskAttributes.getTaskRamMap(this.taskVertexSet));
        //.setTaskDiskMap(taskAttributes.getTaskDiskMap(this.taskVertexSet))
        //.setTaskCpuMap(taskAttributes.getTaskCPUMap(this.taskVertexSet));
  }

  @Override
  public TaskSchedulePlan schedule(DataFlowTaskGraph dataFlowTaskGraph, WorkerPlan workerPlan) {
    this.taskVertexSet = dataFlowTaskGraph.getTaskVertexSet();
    TaskSchedulePlanBuilder taskSchedulePlanBuilder = newTaskSchedulingPlanBuilder(null);
    try {
      taskSchedulePlanBuilder = FirstFitFTaskSchedulingAlgorithm(taskSchedulePlanBuilder);
    } catch (TaskSchedulerException te) {
      throw new TaskSchedulerException(
          "Couldn't allocate all instances to task schedule plan", te);
    }
    return taskSchedulePlanBuilder.build();
  }

  public TaskSchedulePlanBuilder FirstFitFTaskSchedulingAlgorithm(
      TaskSchedulePlanBuilder taskSchedulePlanBuilder) throws TaskSchedulerException {
    Map<String, Integer> parallelTaskMap = taskAttributes.getParallelTaskMap(this.taskVertexSet);
    assignInstancesToContainers(taskSchedulePlanBuilder, parallelTaskMap);
    return taskSchedulePlanBuilder;
  }

  public void assignInstancesToContainers(TaskSchedulePlanBuilder taskSchedulePlanBuilder,
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
    Collections.sort(ramRequirements, Collections.reverseOrder());
    return ramRequirements;
  }

  public void FirstFitInstanceAllocation(TaskSchedulePlanBuilder taskSchedulePlanBuilder,
                                         String taskName) throws TaskSchedulerException {
    if (this.numContainers == 0) {
      taskSchedulePlanBuilder.updateNumContainers(++numContainers);
    }
    try {
      //taskSchedulePlanBuilder.addInstance(taskName);
      taskSchedulePlanBuilder.addInstance(new ContainerIdScorer(), taskName);
    } catch (TaskSchedulerException e) {
      LOG.info("Caught exception and increase the containers:" + numContainers);
      taskSchedulePlanBuilder.updateNumContainers(++numContainers);
      taskSchedulePlanBuilder.addInstance(numContainers, taskName);
    }
  }

  @Override
  public void close() {
  }

  public TaskSchedulePlan tschedule() throws ScheduleException {
    TaskSchedulePlanBuilder taskSchedulePlanBuilder = newTaskSchedulingPlanBuilder(null);
    taskSchedulePlanBuilder = FirstFitFTaskSchedulingAlgorithm(taskSchedulePlanBuilder);
    return taskSchedulePlanBuilder.build();
  }

  //This method will be implemented for future rescheduling case....
  public void reschedule() {
  }
}




