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
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.task.graph.Vertex;
import edu.iu.dsc.tws.tsched.utils.TaskAttributes;

/**
 * This class constructs the task instance map which is based on the required ram, disk, and cpu
 * values of the task graph.
 */
public class TaskInstanceMapCalculation {

  private static final Logger LOG = Logger.getLogger(TaskInstanceMapCalculation.class.getName());

  private static final double DEFAULT_DISK_PADDING_PER_CONTAINER = 12;
  private static final double DEFAULT_RAM_PADDING_PER_CONTAINER = 2;
  private static final double NOT_SPECIFIED_NUMBER_VALUE = -1;
  private static final double DEFAULT_CPU_PADDING_PER_CONTAINER = 1;

  private final Double instanceRAM;
  private final Double instanceDisk;
  private final Double instanceCPU;

  private TaskAttributes taskAttributes;

  public TaskInstanceMapCalculation(Double instanceRam, Double instanceDisk, Double instanceCPU) {
    this.instanceRAM = instanceRam;
    this.instanceDisk = instanceDisk;
    this.instanceCPU = instanceCPU;
    taskAttributes = new TaskAttributes();
  }

  private static int getLargestContainerSize(Map<Integer,
      List<InstanceId>> instancesAllocation) {
    int max = 0;
    for (List<InstanceId> instances : instancesAllocation.values()) {
      if (instances.size() > max) {
        max = instances.size();
      }
    }
    LOG.info("Maximum container value is:\t" + max);
    return max;
  }

  private static double getContainerCpuValue(Map<Integer, List<InstanceId>> instancesAllocation) {

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

  private static Double getContainerDiskValue(Map<Integer, List<InstanceId>> instancesAllocation) {

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

  private static Double getContainerRamValue(Map<Integer, List<InstanceId>> instancesAllocation) {

        /*List<JobAPI.Config.KeyValue> jobConfig = job.getJobConfig().getKvsList();
        return JobUtils.getConfigWithDefault(
                jobConfig, com.tws.api.Config.JOB_CONTAINER_RAM_REQUESTED,
                NOT_SPECIFIED_NUMBER_VALUE);*/

    //These two lines will be once the actual job description file is created.
    Long containerRAMValue = 10L;
    //return ByteAmount.fromGigabytes (containerRAMValue);
    return containerRAMValue.doubleValue();
  }

  /**
   *  It receives the container instance allocation map and calculate the required number of
   *  task instances with ram values.
   * @param containerInstanceAllocationMap
   * @param taskVertexSet
   * @return
   */
  public Map<Integer, Map<InstanceId, Double>> getInstancesRamMapInContainer(
      Map<Integer, List<InstanceId>> containerInstanceAllocationMap, Set<Vertex> taskVertexSet) {

    Map<String, Double> ramMap = taskAttributes.getTaskRamMap(taskVertexSet);
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
          usedRamValue += ramValue;
        } else {
          instancesToBeCalculated.add(instanceId);
        }
      }

      Double containerRamValue = getContainerRamValue(containerInstanceAllocationMap);
      int instancesAllocationSize = instancesToBeCalculated.size();
      if (instancesAllocationSize != 0) {
        Double instanceRequiredRam = instanceRAM;
        if (!containerRamValue.equals(NOT_SPECIFIED_NUMBER_VALUE)) {
          Double remainingRam = containerRamValue - DEFAULT_RAM_PADDING_PER_CONTAINER
              - usedRamValue;
          instanceRequiredRam = remainingRam / instancesAllocationSize;
        }
        for (InstanceId instanceId : instancesToBeCalculated) {
          containerRam.put(instanceId, instanceRequiredRam);
        }
        LOG.info("Instances Required Ram:\t" + instanceRequiredRam + "\n");
      }
    }
    return instancesRamContainerMap;
  }


  /**
   * It receives the container instance allocation map and calculate the required number of
   * instances with disk values.
   * @param containerInstanceAllocationMap
   * @param taskVertexSet
   * @return
   */
  public Map<Integer, Map<InstanceId, Double>> getInstancesDiskMapInContainer(
      Map<Integer, List<InstanceId>> containerInstanceAllocationMap, Set<Vertex> taskVertexSet) {

    Map<String, Double> diskMap = taskAttributes.getTaskDiskMap(taskVertexSet);
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
          usedDiskValue += diskValue;
        } else {
          instancesToBeCalculated.add(instanceId);
        }
      }

      Double containerDiskValue = getContainerDiskValue(containerInstanceAllocationMap);
      int instancesAllocationSize = instancesToBeCalculated.size();
      if (instancesAllocationSize != 0) {
        Double instanceRequiredDisk = 0.0;
        if (!containerDiskValue.equals(NOT_SPECIFIED_NUMBER_VALUE)) {
          Double remainingDisk = containerDiskValue - DEFAULT_DISK_PADDING_PER_CONTAINER
              - usedDiskValue;
          instanceRequiredDisk = remainingDisk / instancesAllocationSize;
        }
        for (InstanceId instanceId : instancesToBeCalculated) {
          containerDisk.put(instanceId, instanceRequiredDisk);
        }
        LOG.info("Instances Required Disk:\t" + instanceRequiredDisk);
      }
    }
    return instancesDiskContainerMap;
  }

  /**
   * It receives the container instance allocation map and calculate the required number of
   * task instances with cpu values.
   * @param containerInstanceAllocationMap
   * @param taskVertexSet
   * @return
   */
  public Map<Integer, Map<InstanceId, Double>> getInstancesCPUMapInContainer(
      Map<Integer, List<InstanceId>> containerInstanceAllocationMap, Set<Vertex> taskVertexSet) {

    Map<String, Double> taskCpuMap = taskAttributes.getTaskCPUMap(taskVertexSet);
    Map<Integer, Map<InstanceId, Double>> instancesCpuContainerMap = new HashMap<>();

    for (int containerId : containerInstanceAllocationMap.keySet()) {
      Double usedCPUValue = 0.0;
      List<InstanceId> instanceIds = containerInstanceAllocationMap.get(containerId);
      Map<InstanceId, Double> containerCPUMap = new HashMap<>();
      instancesCpuContainerMap.put(containerId, containerCPUMap);
      List<InstanceId> instancesToBeCalculated = new ArrayList<>();

      for (InstanceId instanceId : instanceIds) {
        String taskName = instanceId.getTaskName();
        if (taskCpuMap.containsKey(taskName)) {
          Double taskCpuValue = taskCpuMap.get(taskName);
          containerCPUMap.put(instanceId, taskCpuValue);
          usedCPUValue += taskCpuValue;
        } else {
          instancesToBeCalculated.add(instanceId);
        }
      }

      Double containerCpuValue = getContainerCpuValue(containerInstanceAllocationMap);
      int instancesAllocationSize = instancesToBeCalculated.size();
      if (instancesAllocationSize != 0) {
        Double instanceRequiredCpu = 0.0;
        if (!containerCpuValue.equals(NOT_SPECIFIED_NUMBER_VALUE)) {
          Double remainingCpu = containerCpuValue - DEFAULT_CPU_PADDING_PER_CONTAINER
              - usedCPUValue;
          instanceRequiredCpu = remainingCpu / instancesAllocationSize;
        }
        for (InstanceId instanceId : instancesToBeCalculated) {
          containerCPUMap.put(instanceId, instanceRequiredCpu);
        }
        LOG.info("Instances Required CPU:\t" + instanceRequiredCpu);
      }
    }
    return instancesCpuContainerMap;
  }
}

