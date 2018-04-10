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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.tsched.spi.common.TaskConfig;
import edu.iu.dsc.tws.tsched.spi.scheduler.WorkerPlan;
import edu.iu.dsc.tws.tsched.spi.taskschedule.InstanceId;
import edu.iu.dsc.tws.tsched.spi.taskschedule.InstanceMapCalculation;
import edu.iu.dsc.tws.tsched.spi.taskschedule.Resource;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskInstanceId;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedule;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedulePlan;
import edu.iu.dsc.tws.tsched.utils.Job;

public class RoundRobinTaskScheduling implements TaskSchedule {

  private static final Logger LOG = Logger.getLogger(RoundRobinTaskScheduling.class.getName());

  private static final double DEFAULT_DISK_PADDING_PER_CONTAINER = 12;
  private static final double DEFAULT_CPU_PADDING_PER_CONTAINER = 1;
  private static final double MIN_RAM_PER_INSTANCE = 180;
  private static final double DEFAULT_RAM_PADDING_PER_CONTAINER = 2;
  private static final double NOT_SPECIFIED_NUMBER_VALUE = -1;

  private Double instanceRAM;
  private Double instanceDisk;
  private Double instanceCPU;

  private Double containerRAMValue;
  private Double containerDiskValue;
  private Double containerCPUValue;

  private Resource containerMaximumResource;
  private Resource defaultResource;
  private Job job;
  //newly added
  private TaskConfig config;

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

  @Override
  public void initialize(TaskConfig configVal, Job jobObject) {
    this.config = configVal;
    this.job = jobObject;
  }

  @Override
  public void initialize(Job jobObject) {
    this.job = jobObject;
    this.instanceRAM = TaskConfig.containerMaxRAMValue;
    this.instanceCPU = TaskConfig.containerMaxCpuValue;
    this.instanceDisk = TaskConfig.containerMaxDiskValue;
  }

  public void initialize(Config cfg) {

  }

  public TaskSchedulePlan schedule(DataFlowTaskGraph graph, WorkerPlan workerPlan) {
    return null;
  }

  @Override
  public TaskSchedulePlan tschedule() {

    Set<TaskSchedulePlan.ContainerPlan> containerPlans = new HashSet<>();
    Map<Integer, List<InstanceId>> roundRobinContainerInstanceMap =
        RoundRobinScheduling.RoundRobinSchedulingAlgorithm(job);

    InstanceMapCalculation instanceMapCalculation = new InstanceMapCalculation(instanceRAM,
        instanceCPU, instanceDisk);

    Map<Integer, Map<InstanceId, Double>> instancesRamMap =
        instanceMapCalculation.getInstancesRamMapInContainer(roundRobinContainerInstanceMap, job);

    Map<Integer, Map<InstanceId, Double>> instancesDiskMap =
        instanceMapCalculation.getInstancesDiskMapInContainer(roundRobinContainerInstanceMap, job);

    Map<Integer, Map<InstanceId, Double>> instancesCPUMap =
        instanceMapCalculation.getInstancesCPUMapInContainer(roundRobinContainerInstanceMap, job);

    for (int containerId : roundRobinContainerInstanceMap.keySet()) {

      List<InstanceId> taskInstanceIds = roundRobinContainerInstanceMap.get(containerId);
      Map<InstanceId, TaskSchedulePlan.TaskInstancePlan> taskInstancePlanMap = new HashMap<>();

      for (InstanceId id : taskInstanceIds) {

        double instanceRAMValue = instancesRamMap.get(containerId).get(id);
        double instanceDiskValue = instancesDiskMap.get(containerId).get(id);
        double instanceCPUValue = instancesCPUMap.get(containerId).get(id);

        Resource resource = new Resource(instanceRAMValue, instanceDiskValue, instanceCPUValue);
        taskInstancePlanMap.put(id,
            new TaskSchedulePlan.TaskInstancePlan("mpitask", 1, 1, resource));
      }

      Resource resource = new Resource(containerRAMValue, containerDiskValue, containerCPUValue);
      TaskSchedulePlan.ContainerPlan taskContainerPlan =
          new TaskSchedulePlan.ContainerPlan(containerId,
              new HashSet<>(taskInstancePlanMap.values()), resource);
      containerPlans.add(taskContainerPlan);

    }
    return new TaskSchedulePlan(job.getJobId(), containerPlans);
  }

  @Override
  public void close() {
  }
}


