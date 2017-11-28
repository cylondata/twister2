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
package edu.iu.dsc.tws.tsched.RoundRobin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.tsched.spi.common.Config;
import edu.iu.dsc.tws.tsched.spi.taskschedule.InstanceId;
import edu.iu.dsc.tws.tsched.spi.taskschedule.Resource;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedule;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedulePlan;
import edu.iu.dsc.tws.tsched.utils.Job;
import edu.iu.dsc.tws.tsched.utils.JobAttributes;

/**
 * This class is responsible for
 * 1. Initializing the RAM, Disk, and CPU percentage values from the Config and Job files.
 * 2. Perform the Round Robin based scheduling for assigning the instances to the containers.
 * 3. Generate the task schedule plan for the containers and the instances in those containers.
 */
public class RoundRobinTaskScheduling implements TaskSchedule {

  private static final Logger LOG = Logger.getLogger(RoundRobinTaskScheduling.class.getName());

  //It should be replaced with appropriate values....
  private static final double DEFAULT_DISK_PADDING_PER_CONTAINER = 10;
  private static final double DEFAULT_CPU_PADDING_PER_CONTAINER = 1;
  private static final double MIN_RAM_PER_INSTANCE = 248;
  private static final double NOT_SPECIFIED_NUMBER_VALUE = -1;
  private static final double DEFAULT_RAM_PADDING_PER_CONTAINER = 2;
  private Job jobObject;
  private double instanceRAM;
  private double instanceDisk;
  private double instanceCPU;

  @Override
  public void initialize(Config config, Job job) {
    this.jobObject = job;

    //The commented value should be enabled once the context class is created.
    //this.instanceRAM = Context.instanceRam(config);
    //this.instanceDisk = Context.instanceDisk(config);
    //this.instanceCPU = Context.instanceCPU(config);

    this.instanceRAM = Config.containerMaxRAMValue;
    this.instanceCPU = Config.containerMaxCpuValue;
    this.instanceDisk = Config.containerMaxDiskValue;
  }

  /**
   * This method invokes the Round Robin Scheduling Method and fetch the container instance allocation map.
   * Using the map value it calculates the required ram, disk, and cpu percentage for
   * each container and instances in each container and generates the task schedule plan
   * for those instances and the containers.
   */
  @Override
  public TaskSchedulePlan tschedule() {

    Map<Integer, List<InstanceId>> roundRobinContainerInstanceMap = RoundRobinScheduling();
    Set<TaskSchedulePlan.ContainerPlan> containerPlans = new HashSet<>();

    double containerCPUValue = getContainerCPUValue(roundRobinContainerInstanceMap);
    double containerRAMValue = getContainerRamValue(roundRobinContainerInstanceMap);
    double containerDiskValue = getContainerDiskValue(roundRobinContainerInstanceMap);

    for (Integer containerId : roundRobinContainerInstanceMap.keySet()) {

      List<InstanceId> taskInstanceIds = roundRobinContainerInstanceMap.get(containerId);
      Map<InstanceId, TaskSchedulePlan.TaskInstancePlan> taskInstancePlanMap = new HashMap<>();

      for (InstanceId id : taskInstanceIds) {

        double instanceCPUValue = instanceCPU;
        double instanceRAMValue = instanceRAM;
        double instanceDiskValue = instanceDisk;

        Resource resource = new Resource(instanceRAM, instanceDisk, instanceCPU);
        taskInstancePlanMap.put(id,
            new TaskSchedulePlan.TaskInstancePlan("mpitask", 1, 1, resource));

      }
      Resource resource = new Resource(containerRAMValue, containerDiskValue, containerCPUValue);
      TaskSchedulePlan.ContainerPlan taskContainerPlan =
          new TaskSchedulePlan.ContainerPlan(containerId,
              new HashSet<>(taskInstancePlanMap.values()), resource);

      containerPlans.add(taskContainerPlan);
    }
    return new TaskSchedulePlan(jobObject.getJobId(), containerPlans);
  }

  /**
   * This method is to perform the Round Robin based Scheduling operation.
   * And, it will allocate the instances in a Round Robin mode.
   *
   * @return Container Instance Map
   */
  private Map<Integer, List<InstanceId>> RoundRobinScheduling() {

    int taskIndex = 1;
    int globalTaskIndex = 1;
    Job job = new Job();
    job = job.getJob();
    Map<Integer, List<InstanceId>> roundRobinAllocation = new HashMap<>();

    try {
      int numberOfContainers = JobAttributes.getNumberOfContainers(job);
      int totalInstances = JobAttributes.getTotalNumberOfInstances(job);
      for (int i = 1; i <= numberOfContainers; i++) {
        roundRobinAllocation.put(i, new ArrayList<InstanceId>());
      }
      Map<String, Integer> parallelTaskMap = JobAttributes.getParallelTaskMap(job);
      for (String task : parallelTaskMap.keySet()) {
        int numberOfInstances = parallelTaskMap.get(task);
        for (int i = 0; i < numberOfInstances; i++) {
          roundRobinAllocation.get(taskIndex).add(new InstanceId(task, globalTaskIndex, i));
          if (taskIndex != numberOfContainers) {
            taskIndex = taskIndex + 1;
          } else {
            taskIndex = 1;
          }
          globalTaskIndex += 1;
        }
      }
    } catch (NullPointerException ne) {
      ne.printStackTrace();
    }
    return roundRobinAllocation;
  }

  @Override
  public void close() {

  }

  private int getLargestContainerSize(Map<Integer, List<InstanceId>> instancesAllocation) {
    int maximumValue = 0;
    for (List<InstanceId> instances : instancesAllocation.values()) {
      if (instances.size() > maximumValue) {
        maximumValue = instances.size();
      }
    }
    return maximumValue;
  }

  private Map<Integer, Map<InstanceId, Double>> getInstancesRamMapInContainer(
      Map<Integer, List<InstanceId>> containerInstanceAllocationMap) {

    Map<String, Double> ramMap = JobAttributes.getTaskRamMap(this.jobObject);

    Map<Integer, Map<InstanceId, Double>> instancesRamContainerMap = new HashMap<>();
    for (int containerId : containerInstanceAllocationMap.keySet()) {

      Double usedRamValue = 0.0;
      List<InstanceId> instanceIds = containerInstanceAllocationMap.get(containerId);
      Map<InstanceId, Double> containerRam = new HashMap<>();
      instancesRamContainerMap.put(containerId, containerRam);
      List<InstanceId> instancesToBeCalculated = new ArrayList<>();

      for (InstanceId instanceId : instanceIds) {
        String taskName = instanceId.getTaskName();
        if (ramMap.containsKey(taskName)) {
          Double ramValue = ramMap.get(taskName);
          containerRam.put(instanceId, ramValue);
        } else {
          instancesToBeCalculated.add(instanceId);
        }
      }

      Double containerRamValue = getContainerRamValue(containerInstanceAllocationMap);
      int instancesAllocationSize = instancesToBeCalculated.size();
      if (instancesAllocationSize != 0) {
        Double instanceRequiredRam = instanceRAM;

        if (!containerRamValue.equals(NOT_SPECIFIED_NUMBER_VALUE)) {
          Double remainingRam = containerRamValue
              - DEFAULT_RAM_PADDING_PER_CONTAINER - usedRamValue;
          instanceRequiredRam = remainingRam / instancesAllocationSize;

        }
        for (InstanceId instanceId : instancesToBeCalculated) {
          containerRam.put(instanceId, instanceRequiredRam);
        }
      }
    }
    return instancesRamContainerMap;
  }

  private Map<Integer, Map<InstanceId, Double>> getInstancesDiskMapInContainer(
      Map<Integer, List<InstanceId>> containerInstanceAllocationMap) {

    Map<String, Double> diskMap = JobAttributes.getTaskDiskMap(this.jobObject);

    Map<Integer, Map<InstanceId, Double>> instancesDiskContainerMap = new HashMap<>();
    for (int containerId : containerInstanceAllocationMap.keySet()) {

      Double usedDiskValue = 0.0;
      List<InstanceId> instanceIds = containerInstanceAllocationMap.get(containerId);
      Map<InstanceId, Double> containerDisk = new HashMap<>();
      instancesDiskContainerMap.put(containerId, containerDisk);
      List<InstanceId> instancesToBeCalculated = new ArrayList<>();

      for (InstanceId instanceId : instanceIds) {
        String taskName = instanceId.getTaskName();

        if (diskMap.containsKey(taskName)) {
          Double diskValue = diskMap.get(taskName);
          containerDisk.put(instanceId, diskValue);
        } else {
          instancesToBeCalculated.add(instanceId);
        }
      }

      Double containerDiskValue = getContainerDiskValue(containerInstanceAllocationMap);
      int instancesAllocationSize = instancesToBeCalculated.size();
      if (instancesAllocationSize != 0) {
        Double instanceRequiredDisk = 0.0;
        if (!containerDiskValue.equals(NOT_SPECIFIED_NUMBER_VALUE)) {
          Double remainingDisk = containerDiskValue
              - DEFAULT_DISK_PADDING_PER_CONTAINER - usedDiskValue;
          instanceRequiredDisk = remainingDisk / instancesAllocationSize;
        }
        for (InstanceId instanceId : instancesToBeCalculated) {
          containerDisk.put(instanceId, instanceRequiredDisk);
        }
        System.out.println("Instances Required Disk:\t" + instanceRequiredDisk);
      }
    }
    return instancesDiskContainerMap;
  }

  private double getContainerCPUValue(Map<Integer, List<InstanceId>> instancesAllocation) {

    //These two lines will be replaced once the actual job description file is created...
    String cpuHint = "0.6";
    return Double.parseDouble(cpuHint);
  }

  private Double getContainerDiskValue(Map<Integer, List<InstanceId>> instancesAllocation) {

    //These two lines will be replace once the actual job description file is created...
    Long containerDiskValue = 10L;
    return containerDiskValue.doubleValue();
  }

  private Double getContainerRamValue(Map<Integer, List<InstanceId>> instancesAllocation) {

    //These two lines will be replace eonce the actual job description file is created...
    Long containerRAMValue = 10L;
    return containerRAMValue.doubleValue();
  }
}
