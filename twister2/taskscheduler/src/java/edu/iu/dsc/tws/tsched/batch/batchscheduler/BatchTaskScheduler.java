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
package edu.iu.dsc.tws.tsched.batch.batchscheduler;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.api.task.graph.Vertex;
import edu.iu.dsc.tws.api.task.schedule.ITaskScheduler;
import edu.iu.dsc.tws.api.task.schedule.elements.Resource;
import edu.iu.dsc.tws.api.task.schedule.elements.TaskInstanceId;
import edu.iu.dsc.tws.api.task.schedule.elements.TaskInstancePlan;
import edu.iu.dsc.tws.api.task.schedule.elements.TaskSchedulePlan;
import edu.iu.dsc.tws.api.task.schedule.elements.Worker;
import edu.iu.dsc.tws.api.task.schedule.elements.WorkerPlan;
import edu.iu.dsc.tws.api.task.schedule.elements.WorkerSchedulePlan;
import edu.iu.dsc.tws.tsched.spi.common.TaskSchedulerContext;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskInstanceMapCalculation;
import edu.iu.dsc.tws.tsched.utils.DataTransferTimeCalculator;
import edu.iu.dsc.tws.tsched.utils.TaskAttributes;
import edu.iu.dsc.tws.tsched.utils.TaskVertexParser;

public class BatchTaskScheduler implements ITaskScheduler {

  private static final Logger LOG = Logger.getLogger(BatchTaskScheduler.class.getName());

  //Represents global task Id
  private int gTaskId = 0;

  //Represents the task instance ram
  private Double instanceRAM;

  //Represents the task instance disk
  private Double instanceDisk;

  //Represents the task instance cpu value
  private Double instanceCPU;

  //Config object
  private Config config;

  //WorkerId
  private int workerId;

  //Batch Task Allocation Map
  private Map<Integer, List<TaskInstanceId>> batchTaskAllocation;

  //Task Attributes Object
  private TaskAttributes taskAttributes;

  /**
   * This method initialize the task instance values with the values specified in the task config
   * object.
   */
  @Override
  public void initialize(Config cfg) {
    this.config = cfg;
    this.instanceRAM = TaskSchedulerContext.taskInstanceRam(config);
    this.instanceDisk = TaskSchedulerContext.taskInstanceDisk(config);
    this.instanceCPU = TaskSchedulerContext.taskInstanceCpu(config);
    this.batchTaskAllocation = new HashMap<>();
    this.taskAttributes = new TaskAttributes();
  }

  @Override
  public void initialize(Config cfg, int workerid) {
    this.initialize(cfg);
    this.workerId = workerid;
  }


  /**
   * This is the base method for the data locality aware task scheduling for scheduling the batch
   * task instances. It retrieves the task vertex set of the task graph and send the set to the
   * data locality aware scheduling algorithm to allocate the batch task instances which are closer
   * to the data nodes.
   */
  @Override
  public TaskSchedulePlan schedule(DataFlowTaskGraph graph, WorkerPlan workerPlan) {

    LinkedHashMap<Integer, WorkerSchedulePlan> containerPlans = new LinkedHashMap<>();
    for (int i = 0; i < workerPlan.getNumberOfWorkers(); i++) {
      batchTaskAllocation.put(i, new ArrayList<>());
    }

    LinkedHashSet<Vertex> taskVertexSet = new LinkedHashSet<>(graph.getTaskVertexSet());
    TaskVertexParser taskVertexParser = new TaskVertexParser();
    List<Set<Vertex>> taskVertexList = taskVertexParser.parseVertexSet(graph);

    for (Set<Vertex> vertexSet : taskVertexList) {

      Map<Integer, List<TaskInstanceId>> containerInstanceMap;

      if (vertexSet.size() > 1) {
        containerInstanceMap = batchSchedulingAlgorithm(graph, vertexSet, workerPlan);
      } else {
        Vertex vertex = vertexSet.iterator().next();
        containerInstanceMap = batchSchedulingAlgorithm(graph, vertex, workerPlan);
      }

      TaskInstanceMapCalculation instanceMapCalculation = new TaskInstanceMapCalculation(
          this.instanceRAM, this.instanceCPU, this.instanceDisk);

      Map<Integer, Map<TaskInstanceId, Double>> instancesRamMap = instanceMapCalculation.
          getInstancesRamMapInContainer(containerInstanceMap, taskVertexSet);

      Map<Integer, Map<TaskInstanceId, Double>> instancesDiskMap = instanceMapCalculation.
          getInstancesDiskMapInContainer(containerInstanceMap, taskVertexSet);

      Map<Integer, Map<TaskInstanceId, Double>> instancesCPUMap = instanceMapCalculation.
          getInstancesCPUMapInContainer(containerInstanceMap, taskVertexSet);

      for (int containerId : containerInstanceMap.keySet()) {

        double containerRAMValue = TaskSchedulerContext.containerRamPadding(config);
        double containerDiskValue = TaskSchedulerContext.containerDiskPadding(config);
        double containerCpuValue = TaskSchedulerContext.containerCpuPadding(config);

        List<TaskInstanceId> taskTaskInstanceIds = containerInstanceMap.get(containerId);
        Map<TaskInstanceId, TaskInstancePlan> taskInstancePlanMap = new HashMap<>();

        for (TaskInstanceId id : taskTaskInstanceIds) {
          double instanceRAMValue = instancesRamMap.get(containerId).get(id);
          double instanceDiskValue = instancesDiskMap.get(containerId).get(id);
          double instanceCPUValue = instancesCPUMap.get(containerId).get(id);

          Resource instanceResource = new Resource(instanceRAMValue, instanceDiskValue,
              instanceCPUValue);

          taskInstancePlanMap.put(id, new TaskInstancePlan(
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
        } else {
          containerResource = new Resource(containerRAMValue, containerDiskValue,
              containerCpuValue);
        }

        WorkerSchedulePlan taskWorkerSchedulePlan;
        if (containerPlans.containsKey(containerId)) {
          taskWorkerSchedulePlan = containerPlans.get(containerId);
          taskWorkerSchedulePlan.getTaskInstances().addAll(taskInstancePlanMap.values());
        } else {
          taskWorkerSchedulePlan = new WorkerSchedulePlan(containerId, new HashSet<>(
              taskInstancePlanMap.values()), containerResource);
          containerPlans.put(containerId, taskWorkerSchedulePlan);
        }
      }
    }
    TaskSchedulePlan taskSchedulePlan = new TaskSchedulePlan(0,
        new HashSet<>(containerPlans.values()));
    Map<Integer, WorkerSchedulePlan> containersMap = taskSchedulePlan.getContainersMap();
    for (Map.Entry<Integer, WorkerSchedulePlan> entry : containersMap.entrySet()) {
      Integer integer = entry.getKey();
      WorkerSchedulePlan workerSchedulePlan = entry.getValue();
      Set<TaskInstancePlan> containerPlanTaskInstances = workerSchedulePlan.getTaskInstances();
      LOG.info("Task Details for Container Id:" + integer + "\tsize:"
          + containerPlanTaskInstances.size());
      for (TaskInstancePlan ip : containerPlanTaskInstances) {
        LOG.info("TaskId:" + ip.getTaskId() + "\tTask Index" + ip.getTaskIndex()
            + "\tTask Name:" + ip.getTaskName());
      }
    }
    return taskSchedulePlan;
  }

  private Map<Integer, List<TaskInstanceId>> batchSchedulingAlgorithm(
      DataFlowTaskGraph graph, Set<Vertex> vertexSet, WorkerPlan workerPlan) {

    Map<String, Integer> parallelTaskMap;
    if (!graph.getGraphConstraints().isEmpty()) {
      if (!graph.getNodeConstraints().isEmpty()) {
        parallelTaskMap = taskAttributes.getParallelTaskMap(vertexSet, graph.getNodeConstraints());
      } else {
        parallelTaskMap = taskAttributes.getParallelTaskMap(vertexSet);
      }
      batchTaskAllocation = attributeBasedAllocation(parallelTaskMap, graph, workerPlan);
    } else {
      parallelTaskMap = taskAttributes.getParallelTaskMap(vertexSet);
      batchTaskAllocation = nonAttributeBasedAllocation(parallelTaskMap, workerPlan);
    }
    return batchTaskAllocation;
  }

  private Map<Integer, List<TaskInstanceId>> batchSchedulingAlgorithm(
      DataFlowTaskGraph graph, Vertex vertexSet, WorkerPlan workerPlan) {

    Map<String, Integer> parallelTaskMap;
    if (!graph.getGraphConstraints().isEmpty()) {
      if (!graph.getNodeConstraints().isEmpty()) {
        parallelTaskMap = taskAttributes.getParallelTaskMap(vertexSet, graph.getNodeConstraints());
      } else {
        parallelTaskMap = taskAttributes.getParallelTaskMap(vertexSet);
      }
      batchTaskAllocation = attributeBasedAllocation(parallelTaskMap, graph, workerPlan);
    } else {
      parallelTaskMap = taskAttributes.getParallelTaskMap(vertexSet);
      batchTaskAllocation = nonAttributeBasedAllocation(parallelTaskMap, workerPlan);
    }
    return batchTaskAllocation;
  }

  private Map<Integer, List<TaskInstanceId>> attributeBasedAllocation(
      Map<String, Integer> parallelTaskMap, DataFlowTaskGraph graph, WorkerPlan workerPlan) {

    List<DataTransferTimeCalculator> workerNodeList = getWorkerNodeList(workerPlan);
    int containerIndex = Integer.parseInt(workerNodeList.get(0).getNodeName());
    int instancesPerContainer = taskAttributes.getInstancesPerWorker(graph.getGraphConstraints());

    for (Map.Entry<String, Integer> e : parallelTaskMap.entrySet()) {
      String task = e.getKey();
      int taskParallelism = e.getValue();
      for (int taskIndex = 0, maxTaskObject = 0; taskIndex < taskParallelism; taskIndex++) {
        batchTaskAllocation.get(containerIndex).add(
            new TaskInstanceId(task, gTaskId, taskIndex));
        maxTaskObject++;
        if (maxTaskObject == instancesPerContainer) {
          ++containerIndex;
        }
      }
      containerIndex = 0;
      gTaskId++;
    }
    return batchTaskAllocation;
  }

  private Map<Integer, List<TaskInstanceId>> nonAttributeBasedAllocation(
      Map<String, Integer> parallelTaskMap, WorkerPlan workerPlan) {

    List<DataTransferTimeCalculator> workerNodeList = getWorkerNodeList(workerPlan);
    int instancesPerContainer = TaskSchedulerContext.defaultTaskInstancesPerContainer(config);
    int containerIndex = Integer.parseInt(workerNodeList.get(0).getNodeName());

    for (Map.Entry<String, Integer> e : parallelTaskMap.entrySet()) {
      String task = e.getKey();
      int taskParallelism = e.getValue();
      for (int taskIndex = 0, maxTaskObject = 0; taskIndex < taskParallelism; taskIndex++) {
        batchTaskAllocation.get(containerIndex).add(
            new TaskInstanceId(task, gTaskId, taskIndex));
        maxTaskObject++;
        if (maxTaskObject == instancesPerContainer) {
          ++containerIndex;
        }
      }
      containerIndex = 0;
      gTaskId++;
    }
    return batchTaskAllocation;
  }

  private List<DataTransferTimeCalculator> getWorkerNodeList(WorkerPlan workerPlan) {
    return null;
  }
}
