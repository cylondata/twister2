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
import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.iu.dsc.tws.tsched.builder.ContainerIdScorer;
import edu.iu.dsc.tws.tsched.builder.TaskSchedulePlanBuilder;
import edu.iu.dsc.tws.tsched.spi.common.Config;
import edu.iu.dsc.tws.tsched.spi.common.Context;
import edu.iu.dsc.tws.tsched.spi.taskschedule.Resource;
import edu.iu.dsc.tws.tsched.spi.taskschedule.ScheduleException;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskInstanceId;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedulePlan;
import edu.iu.dsc.tws.tsched.utils.Job;
import edu.iu.dsc.tws.tsched.utils.JobAttributes;
import edu.iu.dsc.tws.tsched.utils.RequiredRam;

public class FirstFitTaskScheduling {

  //These values should be replaced with an appropriate values...
  /*private static final double DEFAULT_DISK_PADDING_PER_CONTAINER = 12;
  private static final double DEFAULT_CPU_PADDING_PER_CONTAINER = 1;
  private static final double MIN_RAM_PER_INSTANCE = 180;
  private static final double DEFAULT_RAM_PADDING_PER_CONTAINER = 2;
  private static final double NOT_SPECIFIED_NUMBER_VALUE = -1;
  private static final int DEFAULT_CONTAINER_PADDING_PERCENTAGE = 10;
  private static final int DEFAULT_NUMBER_INSTANCES_PER_CONTAINER = 4;*/
  /////////////////////////////////////////////////////////////////

  private Job jobObject;
  private Config configValue;
  private Resource defaultResourceValue;
  private Resource maximumContainerResourceValue;
  private int paddingPercentage;

  private Double instanceRAM;
  private Double instanceDisk;
  private Double instanceCPU;

  private int numContainers;
  private Double containerRAMValue;
  private Double containerDiskValue;
  private Double containerCPUValue;


  public void initialize(Config config, Job job) {
    this.configValue = config;
    this.jobObject = job;
    this.defaultResourceValue = new Resource(Context.instanceRam(configValue),
        Context.instanceDisk(configValue), Context.instanceCPU(configValue));
    this.paddingPercentage = JobAttributes.JOB_CONTAINER_PADDING_PERCENTAGE;

    instanceRAM = this.defaultResourceValue.getRam();
    instanceDisk = this.defaultResourceValue.getDisk();
    instanceCPU = this.defaultResourceValue.getCpu();

    //This value will be calculated by adding the container percentage value....
    this.maximumContainerResourceValue = new Resource(
        JobAttributes.JOB_CONTAINER_MAX_RAM_VALUE,
        JobAttributes.JOB_CONTAINER_MAX_DISK_VALUE,
        JobAttributes.JOB_CONTAINER_MAX_CPU_VALUE);

    System.out.println("instance default values:" + instanceRAM + "\t"
        + instanceDisk + "\t" + instanceCPU);
  }

  private TaskSchedulePlanBuilder newTaskSchedulingPlanBuilder(TaskSchedulePlan previousTaskPlan) {

    return new TaskSchedulePlanBuilder(jobObject.getJobId(), previousTaskPlan)
        .setContainerMaximumResourceValue(maximumContainerResourceValue)
        .setInstanceDefaultResourceValue(defaultResourceValue)
        .setRequestedContainerPadding(paddingPercentage)
        .setTaskRamMap(JobAttributes.getTaskRamMap(jobObject))
        .setTaskDiskMap(JobAttributes.getTaskDiskMap(jobObject))
        .setTaskCpuMap(JobAttributes.getTaskCPUMap(jobObject));
  }

  public TaskSchedulePlan tschedule() throws ScheduleException {

    TaskSchedulePlanBuilder taskSchedulePlanBuilder = newTaskSchedulingPlanBuilder(null);
    System.out.println("Task schedule plan container maximum ram value is:\t"
        + taskSchedulePlanBuilder.getContainerMaximumResourceValue().getDisk());
    taskSchedulePlanBuilder = FirstFitFTaskSchedulingAlgorithm(taskSchedulePlanBuilder);
    return taskSchedulePlanBuilder.build();

  }

  public TaskSchedulePlanBuilder FirstFitFTaskSchedulingAlgorithm(TaskSchedulePlanBuilder
                                                                      taskSchedulePlanBuilder) {

    Map<String, Integer> parallelTaskMap = JobAttributes.getParallelTaskMap(jobObject);
    assignInstancesToContainers(taskSchedulePlanBuilder, parallelTaskMap);
    System.out.println("Parallel TaskMap:" + parallelTaskMap.keySet());
    return taskSchedulePlanBuilder;

  }

  /*private static int getLargestContainerSize(Map<Integer, List<InstanceId>> InstancesAllocation) {
        int max = 0;
        for (List<InstanceId> instances : InstancesAllocation.values ()) {
            if (instances.size () > max) {
                max = instances.size ();
            }
        }
        System.out.println("Maximum container value is:\t"+max);
        return max;
  }*/

  public void assignInstancesToContainers(TaskSchedulePlanBuilder taskSchedulePlanBuilder,
                                          Map<String, Integer> parallelTaskMap) {

    ArrayList<RequiredRam> ramRequirements = getSortedRAMInstances(parallelTaskMap.keySet());
    for (RequiredRam ramRequirement : ramRequirements) {
      String taskName = ramRequirement.getTaskName();
      int numberOfInstances = parallelTaskMap.get(taskName);
      System.out.println("Number of Instances for the task name:"
          + numberOfInstances + "\t" + taskName);
      for (int j = 0; j < numberOfInstances; j++) {
        FirstFitInstanceAllocation(taskSchedulePlanBuilder, taskName);
        System.out.println("I am inside assign instances to container function:"
            + taskSchedulePlanBuilder.getTaskDiskMap()
            + "\t" + taskName);
      }
    }
  }

  public void FirstFitInstanceAllocation(TaskSchedulePlanBuilder taskSchedulePlanBuilder,
                                         String taskName) {

    if (this.numContainers == 0) {
      taskSchedulePlanBuilder.updateNumContainers(++numContainers);
    }
    try {
      //taskSchedulePlanBuilder.addInstance(taskName);
      taskSchedulePlanBuilder.addInstance(new ContainerIdScorer(), taskName);
    } catch (Exception e) {
      e.printStackTrace();
      taskSchedulePlanBuilder.updateNumContainers(++numContainers);
      taskSchedulePlanBuilder.addInstance(numContainers, taskName);
    }
  }

  private ArrayList<RequiredRam> getSortedRAMInstances(Set<String> taskNameSet) {
    Job job = new Job();
    job.setJob(job);
    ArrayList<RequiredRam> ramRequirements = new ArrayList<>();
    Map<String, Double> taskRamMap = JobAttributes.getTaskRamMap(job);
    for (String taskName : taskNameSet) {
        /*ResourceContainer requiredResource = PackingUtils.getResourceRequirement(
                    taskName, ramMap, this.defaultResourceValue,
                    this.maximumContainerResourceValue, this.paddingPercentage);*/
      //ramRequirements.add(new RequiredRam(taskName, requiredResource.getRam()));

      if (taskRamMap.containsKey(taskName)) {
        instanceRAM = taskRamMap.get(taskName);
      }
      //RequiredRam requiredRam = new RequiredRam (taskName, instanceRAM);
      //ramRequirements.add (requiredRam);
      ramRequirements.add(new RequiredRam(taskName, instanceRAM));
      System.out.println("Task Name and Required Ram:" + taskName + "\t" + instanceRAM);
    }
    Collections.sort(ramRequirements, Collections.reverseOrder());
    return ramRequirements;
  }

  private static double getContainerCpuValue(Map<Integer,
      List<TaskInstanceId>> instAllocation) {
    //These two lines will be replaced once the actual job description file is created...
    String cpuHint = "0.6";
    return Double.parseDouble(cpuHint);
  }

  private static Double getContainerDiskValue(Map<Integer,
      List<TaskInstanceId>> instAllocation) {
    //These two lines will be replaced once the actual job description file is created...
    Long containerDiskValue = 100L;
    return containerDiskValue.doubleValue();
  }

  private static Double getContainerRamValue(Map<Integer,
      List<TaskInstanceId>> instAllocation) {
    //These two lines will be replaced once the actual job description file is created...
    Long containerRAMValue = 10L;
    return containerRAMValue.doubleValue();
  }

  //This method will be implemented for future rescheduling case....
  public void reschedule() {
  }

  public void close() {
  }
}



