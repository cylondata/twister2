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
package edu.iu.dsc.tws.tsched.batch.roundrobin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
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
import edu.iu.dsc.tws.tsched.utils.TaskVertexParser;

public class RoundRobinBatchTaskScheduler implements ITaskScheduler {

  private static final Logger LOG = Logger.getLogger(RoundRobinBatchTaskScheduler.class.getName());

  private static int taskSchedulePlanId = 0;
  private Double instanceRAM;
  private Double instanceDisk;
  private Double instanceCPU;
  private Config cfg;

  @Override
  public void initialize(Config cfg1) {
    this.cfg = cfg1;
    this.instanceRAM = TaskSchedulerContext.taskInstanceRam(cfg);
    this.instanceDisk = TaskSchedulerContext.taskInstanceDisk(cfg);
    this.instanceCPU = TaskSchedulerContext.taskInstanceCpu(cfg);
  }
  private static int gtaskId = 0;

  /**
   * This method generate the container -> instance map
   */
  public static Map<Integer, List<InstanceId>> RoundRobinBatchSchedulingAlgo(
      Vertex taskVertex, int numberOfContainers) {

    TaskAttributes taskAttributes = new TaskAttributes();
    Map<Integer, List<InstanceId>> roundrobinAllocation = new HashMap<>();
    for (int i = 0; i < numberOfContainers; i++) {
      roundrobinAllocation.put(i, new ArrayList<>());
    }

    LOG.fine(String.format("Container Map Values Before Allocation %s", roundrobinAllocation));
    try {
      Map<String, Integer> parallelTaskMap = taskAttributes.getParallelTaskMap(taskVertex);
      int containerIndex = 0;
      for (Map.Entry<String, Integer> e : parallelTaskMap.entrySet()) {
        String task = e.getKey();
        int numberOfInstances = e.getValue();
        for (int taskIndex = 0; taskIndex < numberOfInstances; taskIndex++) {
          roundrobinAllocation.get(containerIndex).add(new InstanceId(task, gtaskId, taskIndex));
          ++containerIndex;
          if (containerIndex >= roundrobinAllocation.size()) {
            containerIndex = 0;
          }
        }
        gtaskId++;
      }
      LOG.fine(String.format("Container Map Values After Allocation %s", roundrobinAllocation));
    } catch (NullPointerException ne) {
      ne.printStackTrace();
    }
    return roundrobinAllocation;
  }

  /**
   * This method generate the container -> instance map
   */
  public static Map<Integer, List<InstanceId>> RoundRobinBatchSchedulingAlgo(
      Set<Vertex> taskVertexSet, int numberOfContainers) {

    TaskAttributes taskAttributes = new TaskAttributes();
    Map<Integer, List<InstanceId>> roundrobinAllocation = new HashMap<>();
    for (int i = 0; i < numberOfContainers; i++) {
      roundrobinAllocation.put(i, new ArrayList<>());
    }

    LOG.fine(String.format("Container Map Values Before Allocation %s", roundrobinAllocation));
    try {
      Map<String, Integer> parallelTaskMap = taskAttributes.getParallelTaskMap(taskVertexSet);
      int taskId = 0;
      int containerIndex = 0;

      for (Map.Entry<String, Integer> e : parallelTaskMap.entrySet()) {
        String task = e.getKey();
        int numberOfInstances = e.getValue();
        for (int taskIndex = 0; taskIndex < numberOfInstances; taskIndex++) {
          //roundrobinAllocation.get(containerIndex).add(new InstanceId(task, taskId, taskIndex));
          roundrobinAllocation.get(containerIndex).add(new InstanceId(task, gtaskId, taskIndex));
          ++containerIndex;
          if (containerIndex >= roundrobinAllocation.size()) {
            containerIndex = 0;
          }
        }
        gtaskId++;
        //taskId++;
      }
      LOG.fine(String.format("Container Map Values After Allocation %s", roundrobinAllocation));
    } catch (NullPointerException ne) {
      ne.printStackTrace();
    }
    return roundrobinAllocation;
  }

  /**
   * This method is responsible for handling the batch dataflow task graph.
   */
  public List<TaskSchedulePlan> scheduleBatch(
      DataFlowTaskGraph dataFlowTaskGraph, WorkerPlan workerPlan) {

    Map<Integer, List<InstanceId>> containerInstanceMap;

    //Set<Vertex> taskVertexSet = dataFlowTaskGraph.getTaskVertexSet();
    Set<Vertex> taskVertexSet = new LinkedHashSet<>(dataFlowTaskGraph.getTaskVertexSet());

    List<TaskSchedulePlan> taskSchedulePlanList = new ArrayList<>();
    List<Set<Vertex>> taskVertexList =
        TaskVertexParser.parseVertexSet(taskVertexSet, dataFlowTaskGraph);

    for (Set<Vertex> vertexSet : taskVertexList) {
      if (vertexSet.size() > 1) {
        containerInstanceMap = RoundRobinBatchSchedulingAlgo(vertexSet,
            workerPlan.getNumberOfWorkers());
      } else {
        Vertex vertex = vertexSet.iterator().next();
        containerInstanceMap = RoundRobinBatchSchedulingAlgo(vertex,
            workerPlan.getNumberOfWorkers());
      }

      Set<TaskSchedulePlan.ContainerPlan> containerPlans = new HashSet<>();

      TaskInstanceMapCalculation instanceMapCalculation = new TaskInstanceMapCalculation(
          this.instanceRAM, this.instanceCPU, this.instanceDisk);

      Map<Integer, Map<InstanceId, Double>> instancesRamMap =
          instanceMapCalculation.getInstancesRamMapInContainer(containerInstanceMap,
              taskVertexSet);

      Map<Integer, Map<InstanceId, Double>> instancesDiskMap =
          instanceMapCalculation.getInstancesDiskMapInContainer(containerInstanceMap,
              taskVertexSet);

      Map<Integer, Map<InstanceId, Double>> instancesCPUMap =
          instanceMapCalculation.getInstancesCPUMapInContainer(containerInstanceMap,
              taskVertexSet);

      for (int containerId : containerInstanceMap.keySet()) {

        Double containerRAMValue = TaskSchedulerContext.containerRamPadding(cfg);
        Double containerDiskValue = TaskSchedulerContext.containerDiskPadding(cfg);
        Double containerCpuValue = TaskSchedulerContext.containerCpuPadding(cfg);

        List<InstanceId> taskInstanceIds = containerInstanceMap.get(containerId);
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

        if (worker != null && worker.getCpu() > 0
            && worker.getDisk() > 0 && worker.getRam() > 0) {
          containerResource = new Resource((double) worker.getRam(),
              (double) worker.getDisk(), (double) worker.getCpu());
          //write into a log file
          LOG.fine("Worker (if loop):" + containerId + "\tRam:"
              + worker.getRam() + "\tDisk:" + worker.getDisk()  //write into a log file
              + "\tCpu:" + worker.getCpu());
        } else {
          containerResource = new Resource(containerRAMValue, containerDiskValue,
              containerCpuValue);
          //write into a log file
          LOG.fine("Worker (else loop):" + containerId
              + "\tRam:" + containerRAMValue     //write into a log file
              + "\tDisk:" + containerDiskValue
              + "\tCpu:" + containerCpuValue);
        }
        if (taskInstancePlanMap.values() != null) {
          TaskSchedulePlan.ContainerPlan taskContainerPlan =
              new TaskSchedulePlan.ContainerPlan(containerId,
                  new HashSet<>(taskInstancePlanMap.values()), containerResource);
          containerPlans.add(taskContainerPlan);
        }
      }
      taskSchedulePlanList.add(new TaskSchedulePlan(taskSchedulePlanId, containerPlans));
      taskSchedulePlanId++;
    }

    for (TaskSchedulePlan taskSchedulePlan : taskSchedulePlanList) {
      Map<Integer, TaskSchedulePlan.ContainerPlan> containersMap
          = taskSchedulePlan.getContainersMap();
      for (Map.Entry<Integer, TaskSchedulePlan.ContainerPlan> entry : containersMap.entrySet()) {
        Integer integer = entry.getKey();
        TaskSchedulePlan.ContainerPlan containerPlan = entry.getValue();
        Set<TaskSchedulePlan.TaskInstancePlan> taskContainerPlan = containerPlan.getTaskInstances();
        for (TaskSchedulePlan.TaskInstancePlan ip : taskContainerPlan) {
          LOG.fine("Task Id:" + ip.getTaskId() + "\tTask Index" + ip.getTaskIndex()
              + "\tTask Name:" + ip.getTaskName() + "\tContainer Id:" + integer);
        }
      }
    }
    return taskSchedulePlanList;
  }

  @Override
  public TaskSchedulePlan schedule(DataFlowTaskGraph dataFlowTaskGraph, WorkerPlan workerPlan) {

    Set<TaskSchedulePlan.ContainerPlan> containerPlans = new HashSet<>();
    Set<Vertex> taskVertexSet = dataFlowTaskGraph.getTaskVertexSet();
    Map<Integer, List<InstanceId>> roundrobinContainerInstanceMap;

    for (Vertex vertex : taskVertexSet) {
      roundrobinContainerInstanceMap = RoundRobinBatchSchedulingAlgo(vertex,
          workerPlan.getNumberOfWorkers());

      TaskInstanceMapCalculation instanceMapCalculation = new TaskInstanceMapCalculation(
          this.instanceRAM, this.instanceCPU, this.instanceDisk);

      Map<Integer, Map<InstanceId, Double>> instancesRamMap =
          instanceMapCalculation.getInstancesRamMapInContainer(roundrobinContainerInstanceMap,
              taskVertexSet);

      Map<Integer, Map<InstanceId, Double>> instancesDiskMap =
          instanceMapCalculation.getInstancesDiskMapInContainer(roundrobinContainerInstanceMap,
              taskVertexSet);

      Map<Integer, Map<InstanceId, Double>> instancesCPUMap =
          instanceMapCalculation.getInstancesCPUMapInContainer(roundrobinContainerInstanceMap,
              taskVertexSet);

      for (int containerId : roundrobinContainerInstanceMap.keySet()) {

        Double containerRAMValue = TaskSchedulerContext.containerRamPadding(cfg);
        Double containerDiskValue = TaskSchedulerContext.containerDiskPadding(cfg);
        Double containerCpuValue = TaskSchedulerContext.containerCpuPadding(cfg);

        List<InstanceId> taskInstanceIds = roundrobinContainerInstanceMap.get(containerId);
        Map<InstanceId, TaskSchedulePlan.TaskInstancePlan> taskInstancePlanMap = new HashMap<>();

        for (InstanceId id : taskInstanceIds) {
          double instanceRAMValue = instancesRamMap.get(containerId).get(id);
          double instanceDiskValue = instancesDiskMap.get(containerId).get(id);
          double instanceCPUValue = instancesCPUMap.get(containerId).get(id);

          LOG.info("Container Id:" + containerId
              + "Task Id and Index\t" + id.getTaskId() + "\t" + id.getTaskIndex()
              + "\tand Req. Resource:" + instanceRAMValue + "\t" + instanceDiskValue
              + "\t" + instanceCPUValue);

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
          //write into a log file
          LOG.fine("Worker (if loop):" + containerId + "\tRam:"
              + worker.getRam() + "\tDisk:" + worker.getDisk()  //write into a log file
              + "\tCpu:" + worker.getCpu());
        } else {
          containerResource = new Resource(containerRAMValue, containerDiskValue,
              containerCpuValue);
          //write into a log file
          LOG.fine("Worker (else loop):" + containerId
              + "\tRam:" + containerRAMValue     //write into a log file
              + "\tDisk:" + containerDiskValue
              + "\tCpu:" + containerCpuValue);
        }

        TaskSchedulePlan.ContainerPlan taskContainerPlan =
            new TaskSchedulePlan.ContainerPlan(containerId,
                new HashSet<>(taskInstancePlanMap.values()), containerResource);
        containerPlans.add(taskContainerPlan);
      }
    }
    //new TaskSchedulePlan(job.getJobId(), containerPlans);
    return new TaskSchedulePlan(taskSchedulePlanId, containerPlans);
  }
}
