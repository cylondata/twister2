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
import edu.iu.dsc.tws.tsched.utils.TaskVertexParser;

/**
 * This class allocate the task instances into the container in a round robin manner.
 * First, it will allocate the task instances into the logical container values and then
 * it will calculate the required ram, disk, and cpu values for task instances and the logical
 * containers which is based on the task configuration values and the allocated work values
 * respectively.
 * <p>
 * For example, if there are two tasks with parallelism value of 2, 1st task -> instance 0 will
 * go to container 0, 2nd task -> instance 0 will go to container 1, 1st task -> instance 1 will
 * go to container 1, 2nd task -> instance 1 will go to container 1.
 */

public class RoundRobinBatchTaskScheduler implements ITaskScheduler {

  private static final Logger LOG = Logger.getLogger(RoundRobinBatchTaskScheduler.class.getName());

  //Represents the task schedule plan Id
  private static int taskSchedulePlanId = 0;

  //Represents global task Id
  private static int gtaskId = 0;

  //Represents the task instance ram
  private Double instanceRAM;

  //Represents the task instance disk
  private Double instanceDisk;

  //Represents the task instance cpu value
  private Double instanceCPU;

  //Config object
  private Config config;

  /**
   * This method retrieves the parallel task map and the total number of task instances for the
   * task vertex. Then, it will allocate the instances into the number of containers allocated for
   * the task in a round robin fashion.
   */
  private static Map<Integer, List<InstanceId>> roundRobinBatchSchedulingAlgorithm(
          Vertex taskVertex, int numberOfContainers) {

    TaskAttributes taskAttributes = new TaskAttributes();
    Map<Integer, List<InstanceId>> roundrobinAllocation = new HashMap<>();

    for (int i = 0; i < numberOfContainers; i++) {
      roundrobinAllocation.put(i, new ArrayList<>());
    }

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
    } catch (NullPointerException ne) {
      ne.printStackTrace();
    }
    return roundrobinAllocation;
  }

  /**
   * This method retrieves the parallel task map and the total number of task instances for the
   * task vertex set. Then, it will allocate the instances into the number of containers allocated
   * for the task in a round robin fashion.
   */
  private static Map<Integer, List<InstanceId>> roundRobinBatchSchedulingAlgorithm(
          Set<Vertex> taskVertexSet, int numberOfContainers) {

    TaskAttributes taskAttributes = new TaskAttributes();
    Map<Integer, List<InstanceId>> roundrobinAllocation = new HashMap<>();
    for (int i = 0; i < numberOfContainers; i++) {
      roundrobinAllocation.put(i, new ArrayList<>());
    }

    try {
      Map<String, Integer> parallelTaskMap = taskAttributes.getParallelTaskMap(taskVertexSet);
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
    } catch (NullPointerException ne) {
      ne.printStackTrace();
    }
    return roundrobinAllocation;
  }

  /**
   * This method initialize the task instance values with the values specified in the task config
   * object.
   *
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
   * This is the base method which receives the dataflow taskgraph and the worker plan to allocate
   * the task instances to the appropriate workers with their required ram, disk, and cpu values.
   *
   * @param dataFlowTaskGraph
   * @param workerPlan
   * @return
   */
  @Override
  public TaskSchedulePlan schedule(DataFlowTaskGraph dataFlowTaskGraph, WorkerPlan workerPlan) {

    Map<Integer, List<InstanceId>> containerInstanceMap;

    TaskSchedulePlan taskSchedulePlan;

    Map<Integer, TaskSchedulePlan.ContainerPlan> containerPlans = new LinkedHashMap<>();

    /*To retrieve the batch task instances(it may be single task vertex or a batch
    of task vertexes)*/

    List<Set<Vertex>> taskVertexList = TaskVertexParser.parseVertexSet(dataFlowTaskGraph);

    //Set<Vertex> taskVertexSet = dataFlowTaskGraph.getTaskVertexSet();
    Set<Vertex> taskVertexSet = new LinkedHashSet<>(dataFlowTaskGraph.getTaskVertexSet());

    for (Set<Vertex> vertexSet : taskVertexList) {

      /* Based on the size of the task vertex list, it will invoke the respective methods */
      if (vertexSet.size() > 1) {
        containerInstanceMap = roundRobinBatchSchedulingAlgorithm(vertexSet,
                workerPlan.getNumberOfWorkers());
      } else {
        Vertex vertex = vertexSet.iterator().next();
        containerInstanceMap = roundRobinBatchSchedulingAlgorithm(vertex,
                workerPlan.getNumberOfWorkers());
      }

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

        double containerRAMValue = TaskSchedulerContext.containerRamPadding(config);
        double containerDiskValue = TaskSchedulerContext.containerDiskPadding(config);
        double containerCpuValue = TaskSchedulerContext.containerCpuPadding(config);

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
          LOG.fine("Worker (if loop):" + containerId + "\tRam:" + worker.getRam()
                  + "\tDisk:" + worker.getDisk() + "\tCpu:" + worker.getCpu());
        } else {
          containerResource = new Resource(containerRAMValue, containerDiskValue,
                  containerCpuValue);
          LOG.fine("Worker (else loop):" + containerId + "\tRam:" + containerRAMValue
                  + "\tDisk:" + containerDiskValue + "\tCpu:" + containerCpuValue);
        }

        TaskSchedulePlan.ContainerPlan taskContainerPlan;
        if (containerPlans.containsKey(containerId)) {
          taskContainerPlan = containerPlans.get(containerId);
          taskContainerPlan.getTaskInstances().addAll(taskInstancePlanMap.values());
        } else {
          taskContainerPlan =
                  new TaskSchedulePlan.ContainerPlan(containerId,
                          new HashSet<>(taskInstancePlanMap.values()),
                          containerResource);
          containerPlans.put(containerId, taskContainerPlan);
        }
      }
    }

    taskSchedulePlan = new TaskSchedulePlan(taskSchedulePlanId, new HashSet<>(
            containerPlans.values()));

    Map<Integer, TaskSchedulePlan.ContainerPlan> containersMap
            = taskSchedulePlan.getContainersMap();
    for (Map.Entry<Integer, TaskSchedulePlan.ContainerPlan> entry : containersMap.entrySet()) {
      Integer integer = entry.getKey();
      TaskSchedulePlan.ContainerPlan containerPlan = entry.getValue();
      Set<TaskSchedulePlan.TaskInstancePlan> taskInstancePlans
              = containerPlan.getTaskInstances();
      //int containerId = containerPlan.getRequiredResource().getId();
      LOG.fine("Container Index (Schedule Method):" + integer);
      for (TaskSchedulePlan.TaskInstancePlan ip : taskInstancePlans) {
        LOG.fine("Task Id:" + ip.getTaskId() + "\tTask Index" + ip.getTaskIndex()
                + "\tTask Name:" + ip.getTaskName() + "\tContainer Index:" + integer);
      }
    }
    return taskSchedulePlan;
  }

  /**
   * This is the base method which receives the dataflow taskgraph and the worker plan to allocate
   * the task instances to the appropriate workers with their required ram, disk, and cpu values.
   * This method is mainly responsible for returning the task schedule plan as a list based on the
   * batch of tasks.
   *
   * @param dataFlowTaskGraph
   * @param workerPlan
   * @return
   */
  public List<TaskSchedulePlan> scheduleBatch(DataFlowTaskGraph dataFlowTaskGraph,
                                              WorkerPlan workerPlan) {

    Map<Integer, List<InstanceId>> containerInstanceMap;
    List<TaskSchedulePlan> taskSchedulePlanList = new ArrayList<>();

    //Set<Vertex> taskVertexSet = dataFlowTaskGraph.getTaskVertexSet();
    Set<Vertex> taskVertexSet = new LinkedHashSet<>(dataFlowTaskGraph.getTaskVertexSet());

    /* To retrieve the batch task instances(it may be single task vertex or a batch of
    task vertexes */
    List<Set<Vertex>> taskVertexList = TaskVertexParser.parseVertexSet(dataFlowTaskGraph);

    for (Set<Vertex> vertexSet : taskVertexList) {

      /* Based on the size of the task vertex list, it will invoke the respective methods */
      if (vertexSet.size() > 1) {
        containerInstanceMap = roundRobinBatchSchedulingAlgorithm(vertexSet,
                workerPlan.getNumberOfWorkers());
      } else {
        Vertex vertex = vertexSet.iterator().next();
        containerInstanceMap = roundRobinBatchSchedulingAlgorithm(vertex,
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

        double containerRAMValue = TaskSchedulerContext.containerRamPadding(config);
        double containerDiskValue = TaskSchedulerContext.containerDiskPadding(config);
        double containerCpuValue = TaskSchedulerContext.containerCpuPadding(config);

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
          LOG.fine("Worker (if loop):" + containerId + "\tRam:" + worker.getRam()
                  + "\tDisk:" + worker.getDisk() + "\tCpu:" + worker.getCpu());
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
        LOG.fine("Container Index (ScheduleBatch Method):" + integer);
        for (TaskSchedulePlan.TaskInstancePlan ip : taskContainerPlan) {
          LOG.fine("Task Id:" + ip.getTaskId() + "\tTask Index" + ip.getTaskIndex()
                  + "\tTask Name:" + ip.getTaskName() + "\tContainer Index:" + integer);
        }
      }
    }
    return taskSchedulePlanList;
  }
}

