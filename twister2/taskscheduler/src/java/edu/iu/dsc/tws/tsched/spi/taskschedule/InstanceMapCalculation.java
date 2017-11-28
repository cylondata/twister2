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
package edu.iu.dsc.tws.tsched.spi.taskschedule;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.iu.dsc.tws.tsched.utils.Job;
import edu.iu.dsc.tws.tsched.utils.JobAttributes;

public class InstanceMapCalculation {

  private static final double DEFAULT_DISK_PADDING_PER_CONTAINER = 12;
  private static final double DEFAULT_CPU_PADDING_PER_CONTAINER = 1;
  private static final double MIN_RAM_PER_INSTANCE = 180;
  private static final double DEFAULT_RAM_PADDING_PER_CONTAINER = 2;
  private static final double NOT_SPECIFIED_NUMBER_VALUE = -1;

  private final Double instanceRAM;
  private final Double instanceDisk;
  private final Double instanceCPU;

  public InstanceMapCalculation(Double instanceRam, Double instanceDisk, Double instanceCPU) {
    this.instanceRAM = instanceRam;
    this.instanceDisk = instanceDisk;
    this.instanceCPU = instanceCPU;
  }

  private static double getContainerCpuValue(Map<Integer,
      List<TaskInstanceId>> instancesAllocation) {

        /*List<JobAPI.Config.KeyValue> jobConfig= job.getJobConfig().getKvsList();
        double defaultContainerCpu =
                DEFAULT_CPU_PADDING_PER_CONTAINER + getLargestContainerSize(instancesAllocation);

        String cpuHint = JobUtils.getConfigWithDefault(
                jobConfig, com.tws.api.Config.TOPOLOGY_CONTAINER_CPU_REQUESTED,
                Double.toString(defaultContainerCpu)); */

    //These two lines will be once the actual job description file is created.
    String cpuHint = "0.6";
    return Double.parseDouble(cpuHint);
  }

  private static Double getContainerDiskValue(Map<Integer,
      List<TaskInstanceId>> instancesAllocation) {

        /*ByteAmount defaultContainerDisk = instanceDiskDefault
                .multiply(getLargestContainerSize(instancesAllocation))
                .plus(DEFAULT_DISK_PADDING_PER_CONTAINER);

        List<JobAPI.Config.KeyValue> jobConfig = job.getJobConfig().getKvsList();
        return JobUtils.getConfigWithDefault(jobConfig,
                com.tws.api.Config.JOB_CONTAINER_DISK_REQUESTED,
                defaultContainerDisk); */

    //These two lines will be once the actual job description file is created.
    Long containerDiskValue = 100L;
    return containerDiskValue.doubleValue();
  }

  private static Double getContainerRamValue(Map<Integer,
      List<TaskInstanceId>> instancesAllocation) {

        /*List<JobAPI.Config.KeyValue> jobConfig = job.getJobConfig().getKvsList();
        return JobUtils.getConfigWithDefault(
                jobConfig, com.tws.api.Config.JOB_CONTAINER_RAM_REQUESTED,
                NOT_SPECIFIED_NUMBER_VALUE);*/

    //These two lines will be once the actual job description file is created.
    Long containerRAMValue = 10L;
    //return ByteAmount.fromGigabytes (containerRAMValue);
    return containerRAMValue.doubleValue();
  }

  private static int getLargestContainerSize(Map<Integer,
      List<TaskInstanceId>> instancesAllocation) {
    int max = 0;
    for (List<TaskInstanceId> instances : instancesAllocation.values()) {
      if (instances.size() > max) {
        max = instances.size();
      }
    }
    System.out.println("Maximum container value is:\t" + max);
    return max;
  }

  Map<Integer, Map<TaskInstanceId, Double>> getInstancesRamMapInContainer(
      Map<Integer, List<TaskInstanceId>> containerInstanceAllocationMap) {

    Job job = new Job();
    job.setJob(job);

    Map<String, Double> ramMap = JobAttributes.getTaskRamMap(job);
    Map<Integer, Map<TaskInstanceId, Double>> instancesRamContainerMap = new HashMap<>();

    for (int containerId : containerInstanceAllocationMap.keySet()) {
      Double usedRamValue = 0.0;
      List<TaskInstanceId> instanceIds = containerInstanceAllocationMap.get(containerId);
      Map<TaskInstanceId, Double> containerRam = new HashMap<>();
      instancesRamContainerMap.put(containerId, containerRam);
      List<TaskInstanceId> instancesToBeCalculated = new ArrayList<>();

      for (TaskInstanceId instanceId : instanceIds) {
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
        for (TaskInstanceId instanceId : instancesToBeCalculated) {
          containerRam.put(instanceId, instanceRequiredRam);
        }
        System.out.println("Instances Required Ram:\t" + instanceRequiredRam);
      }
    }
    return instancesRamContainerMap;
  }

  Map<Integer, Map<TaskInstanceId, Double>> getInstancesDiskMapInContainer(
      Map<Integer, List<TaskInstanceId>> containerInstanceAllocationMap) {

    Job job = new Job();
    job.setJob(job);

    Map<String, Double> diskMap = JobAttributes.getTaskDiskMap(job);
    Map<Integer, Map<TaskInstanceId, Double>> instancesDiskContainerMap = new HashMap<>();

    for (int containerId : containerInstanceAllocationMap.keySet()) {
      Double usedDiskValue = 0.0;
      List<TaskInstanceId> instanceIds = containerInstanceAllocationMap.get(containerId);
      Map<TaskInstanceId, Double> containerDisk = new HashMap<>();
      instancesDiskContainerMap.put(containerId, containerDisk);
      List<TaskInstanceId> instancesToBeCalculated = new ArrayList<>();

      for (TaskInstanceId instanceId : instanceIds) {
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
        for (TaskInstanceId instanceId : instancesToBeCalculated) {
          containerDisk.put(instanceId, instanceRequiredDisk);
        }
        System.out.println("Instances Required Disk:\t" + instanceRequiredDisk);
      }
    }
    return instancesDiskContainerMap;
  }

  Map<Integer, Map<TaskInstanceId, Double>> getInstancesCPUMapInContainer(
      Map<Integer, List<TaskInstanceId>> containerInstanceAllocationMap) {

    Job job = new Job();
    job.setJob(job);

    Map<String, Double> diskMap = JobAttributes.getTaskCPUMap(job);
    Map<Integer, Map<TaskInstanceId, Double>> instancesCpuContainerMap = new HashMap<>();

    //This for loop should be edited with appropriate values....!
    for (int containerId : containerInstanceAllocationMap.keySet()) {
      Double usedDiskValue = 0.0;
      List<TaskInstanceId> instanceIds = containerInstanceAllocationMap.get(containerId);
      Map<TaskInstanceId, Double> containerDisk = new HashMap<>();
      instancesCpuContainerMap.put(containerId, containerDisk);
      List<TaskInstanceId> instancesToBeCalculated = new ArrayList<>();

      for (TaskInstanceId instanceId : instanceIds) {
        String taskName = instanceId.getTaskName();

        if (diskMap.containsKey(taskName)) {
          Double diskValue = diskMap.get(taskName);
          containerDisk.put(instanceId, diskValue);
        } else {
          instancesToBeCalculated.add(instanceId);
        }
      }

      Double containerDiskValue = getContainerCpuValue(containerInstanceAllocationMap);
      int instancesAllocationSize = instancesToBeCalculated.size();
      if (instancesAllocationSize != 0) {
        Double instanceRequiredDisk = 0.0;
        if (!containerDiskValue.equals(NOT_SPECIFIED_NUMBER_VALUE)) {
          Double remainingRam = containerDiskValue
              - DEFAULT_DISK_PADDING_PER_CONTAINER - usedDiskValue;
          instanceRequiredDisk = remainingRam / instancesAllocationSize;
        }
        for (TaskInstanceId instanceId : instancesToBeCalculated) {
          containerDisk.put(instanceId, instanceRequiredDisk);
        }
        System.out.println("Instances Required CPU:\t" + instanceRequiredDisk);
      }
    }
    return instancesCpuContainerMap;
  }
}
