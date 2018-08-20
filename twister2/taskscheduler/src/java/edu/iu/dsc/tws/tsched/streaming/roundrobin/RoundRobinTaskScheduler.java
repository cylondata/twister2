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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.Vertex;
import edu.iu.dsc.tws.tsched.spi.common.TaskSchedulerContext;
import edu.iu.dsc.tws.tsched.spi.scheduler.Worker;
import edu.iu.dsc.tws.tsched.spi.scheduler.WorkerPlan;
import edu.iu.dsc.tws.tsched.spi.taskschedule.ITaskScheduler;
import edu.iu.dsc.tws.tsched.spi.taskschedule.InstanceId;
import edu.iu.dsc.tws.tsched.spi.taskschedule.Resource;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskInstanceMapCalculation;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedulePlan;
import edu.iu.dsc.tws.tsched.utils.TaskAttributes;

public class RoundRobinTaskScheduler implements ITaskScheduler {

  private static final Logger LOG = Logger.getLogger(RoundRobinTaskScheduler.class.getName());

  private static int taskSchedulePlanId = 0;

  //It represents the task instance ram
  private Double instanceRAM;

  //It represents the task instance disk
  private Double instanceDisk;

  //It represents the task instance cpu value
  private Double instanceCPU;

  private Config config;

  /**
   * This method initialize the task instance values with the values specified in
   * the config object.
   * @param cfg
   */
  @Override
  public void initialize(Config cfg) {
    this.config = cfg;
    this.instanceRAM = TaskSchedulerContext.taskInstanceRam(config);
    this.instanceDisk = TaskSchedulerContext.taskInstanceDisk(config);
    this.instanceCPU = TaskSchedulerContext.taskInstanceCpu(config);
  }

  /**
   * This is the base method which receives the dataflow taskgraph and workerplan
   * to allocate the task instances to the appropriate workers.
   * @param dataFlowTaskGraph
   * @param workerPlan
   * @return
   */

  @Override
  public TaskSchedulePlan schedule(DataFlowTaskGraph dataFlowTaskGraph, WorkerPlan workerPlan) {

    //Allocate the task instances into the containers/workers
    Set<TaskSchedulePlan.ContainerPlan> containerPlans = new LinkedHashSet<>();

    //To get the vertex set from the taskgraph
    Set<Vertex> taskVertexSet = dataFlowTaskGraph.getTaskVertexSet();

    Map<Integer, List<InstanceId>> roundRobinContainerInstanceMap =
        roundRobinSchedulingAlgorithm(taskVertexSet, workerPlan.getNumberOfWorkers());

    TaskInstanceMapCalculation instanceMapCalculation = new TaskInstanceMapCalculation(
        this.instanceRAM, this.instanceCPU, this.instanceDisk);

    Map<Integer, Map<InstanceId, Double>> instancesRamMap =
        instanceMapCalculation.getInstancesRamMapInContainer(roundRobinContainerInstanceMap,
            taskVertexSet);

    Map<Integer, Map<InstanceId, Double>> instancesDiskMap =
        instanceMapCalculation.getInstancesDiskMapInContainer(roundRobinContainerInstanceMap,
            taskVertexSet);

    Map<Integer, Map<InstanceId, Double>> instancesCPUMap =
        instanceMapCalculation.getInstancesCPUMapInContainer(roundRobinContainerInstanceMap,
            taskVertexSet);

    for (int containerId : roundRobinContainerInstanceMap.keySet()) {

      double containerRAMValue = TaskSchedulerContext.containerRamPadding(config);
      double containerDiskValue = TaskSchedulerContext.containerDiskPadding(config);
      double containerCpuValue = TaskSchedulerContext.containerCpuPadding(config);

      List<InstanceId> taskInstanceIds = roundRobinContainerInstanceMap.get(containerId);
      Map<InstanceId, TaskSchedulePlan.TaskInstancePlan> taskInstancePlanMap = new HashMap<>();

      for (InstanceId id : taskInstanceIds) {

        double instanceRAMValue = instancesRamMap.get(containerId).get(id);
        double instanceDiskValue = instancesDiskMap.get(containerId).get(id);
        double instanceCPUValue = instancesCPUMap.get(containerId).get(id);

        Resource instanceResource = new Resource(instanceRAMValue,
            instanceDiskValue, instanceCPUValue);

        taskInstancePlanMap.put(id, new TaskSchedulePlan.TaskInstancePlan(
            id.getTaskName(), id.getTaskId(), id.getTaskIndex(), instanceResource));

        containerRAMValue += instanceRAMValue;
        containerDiskValue += instanceDiskValue;
        containerCpuValue += instanceDiskValue;
      }

      Worker worker = workerPlan.getWorker(containerId);
      Resource containerResource;

      if (worker != null && worker.getCpu() > 0 && worker.getDisk() > 0 && worker.getRam() > 0) {
        containerResource = new Resource((double) worker.getRam(),
            (double) worker.getDisk(), (double) worker.getCpu());
        LOG.fine("Worker (if loop):" + containerId + "\tRam:" + worker.getRam()
            + "\tDisk:" + worker.getDisk()
            + "\tCpu:" + worker.getCpu());
      } else {
        containerResource = new Resource(containerRAMValue, containerDiskValue,
            containerCpuValue);
        LOG.fine("Worker (else loop):" + containerId + "\tRam:" + containerRAMValue
            + "\tDisk:" + containerDiskValue + "\tCpu:" + containerCpuValue);
      }

      TaskSchedulePlan.ContainerPlan taskContainerPlan =
          new TaskSchedulePlan.ContainerPlan(containerId,
              new HashSet<>(taskInstancePlanMap.values()), containerResource);
      containerPlans.add(taskContainerPlan);
    }
    //new TaskSchedulePlan(job.getJobId(), containerPlans);
    return new TaskSchedulePlan(taskSchedulePlanId, containerPlans);
  }

  public static Map<Integer, List<InstanceId>> roundRobinSchedulingAlgorithm(
      Set<Vertex> taskVertexSet, int numberOfContainers) {

    TaskAttributes taskAttributes = new TaskAttributes();
    Map<Integer, List<InstanceId>> roundrobinAllocation = new LinkedHashMap<>();
    for (int i = 0; i < numberOfContainers; i++) {
      roundrobinAllocation.put(i, new ArrayList<>());
    }
    try {
      Map<String, Integer> parallelTaskMap = taskAttributes.getParallelTaskMap(taskVertexSet);
      int totalTaskInstances = taskAttributes.getTotalNumberOfInstances(taskVertexSet);
      if (numberOfContainers < totalTaskInstances) {
        LOG.info(String.format("Container Map Values Before Allocation %s", roundrobinAllocation));
        int globalTaskIndex = 0;
        for (Map.Entry<String, Integer> e : parallelTaskMap.entrySet()) {
          String task = e.getKey();
          int numberOfInstances = e.getValue();
          int containerIndex;
          for (int i = 0; i < numberOfInstances; i++) {
            containerIndex = i % numberOfContainers;
            roundrobinAllocation.get(containerIndex).add(new InstanceId(task, globalTaskIndex, i));
          }
          globalTaskIndex++;
        }
      }

      LOG.info(String.format("Container Map Values After Allocation %s", roundrobinAllocation));
      for (Map.Entry<Integer, List<InstanceId>> entry : roundrobinAllocation.entrySet()) {
        Integer integer = entry.getKey();
        List<InstanceId> instanceIds = entry.getValue();
        LOG.fine("Container Index:" + integer);
        for (InstanceId instanceId : instanceIds) {
          LOG.fine("Task Instance Details:"
              + "\t Task Name:" + instanceId.getTaskName()
              + "\t Task id:" + instanceId.getTaskId()
              + "\t Task index:" + instanceId.getTaskIndex());
        }
      }
    } catch (NullPointerException ne) {
      ne.printStackTrace();
    }
    return roundrobinAllocation;
  }
}

