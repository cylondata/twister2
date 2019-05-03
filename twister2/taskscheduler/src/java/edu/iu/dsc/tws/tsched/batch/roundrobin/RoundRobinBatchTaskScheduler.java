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
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.task.api.schedule.ContainerPlan;
import edu.iu.dsc.tws.task.api.schedule.Resource;
import edu.iu.dsc.tws.task.api.schedule.TaskInstancePlan;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.Vertex;
import edu.iu.dsc.tws.tsched.spi.common.TaskSchedulerContext;
import edu.iu.dsc.tws.tsched.spi.scheduler.Worker;
import edu.iu.dsc.tws.tsched.spi.scheduler.WorkerPlan;
import edu.iu.dsc.tws.tsched.spi.taskschedule.ITaskScheduler;
import edu.iu.dsc.tws.tsched.spi.taskschedule.InstanceId;
import edu.iu.dsc.tws.tsched.spi.taskschedule.ScheduleException;
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

  //Round Robin Allocation Map
  private Map<Integer, List<InstanceId>> roundRobinAllocation;

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
    this.roundRobinAllocation = new HashMap<>();
    this.taskAttributes = new TaskAttributes();
  }

  @Override
  public void initialize(Config cfg, int workerid) {
    this.initialize(cfg);
    this.workerId = workerid;
  }

  /**
   * This is the base method which receives the dataflow taskgraph and the worker plan to allocate
   * the task instances to the appropriate workers with their required ram, disk, and cpu values.
   */
  @Override
  public TaskSchedulePlan schedule(DataFlowTaskGraph dataFlowTaskGraph, WorkerPlan workerPlan) {

    Map<Integer, List<InstanceId>> containerInstanceMap;
    Map<Integer, ContainerPlan> containerPlans = new LinkedHashMap<>();

    for (int i = 0; i < workerPlan.getNumberOfWorkers(); i++) {
      roundRobinAllocation.put(i, new ArrayList<>());
    }

    //To retrieve the batch task instances(it may be single task vertex or a batch of task vertices)
    Set<Vertex> taskVertexSet = new LinkedHashSet<>(dataFlowTaskGraph.getTaskVertexSet());
    TaskVertexParser taskGraphParser = new TaskVertexParser();
    List<Set<Vertex>> taskVertexList = taskGraphParser.parseVertexSet(dataFlowTaskGraph);

    for (Set<Vertex> vertexSet : taskVertexList) {
      if (vertexSet.size() > 1) {
        containerInstanceMap = roundRobinBatchSchedulingAlgorithm(dataFlowTaskGraph, vertexSet);
      } else {
        Vertex vertex = vertexSet.iterator().next();
        containerInstanceMap = roundRobinBatchSchedulingAlgorithm(dataFlowTaskGraph, vertex);
      }

      TaskInstanceMapCalculation instanceMapCalculation =
          new TaskInstanceMapCalculation(this.instanceRAM, this.instanceCPU, this.instanceDisk);

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
        Map<InstanceId, TaskInstancePlan> taskInstancePlanMap = new HashMap<>();

        for (InstanceId id : taskInstanceIds) {

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

        if (worker != null && worker.getCpu() > 0
            && worker.getDisk() > 0 && worker.getRam() > 0) {
          containerResource = new Resource((double) worker.getRam(),
              (double) worker.getDisk(), (double) worker.getCpu());
        } else {
          containerResource = new Resource(containerRAMValue, containerDiskValue,
              containerCpuValue);
        }

        ContainerPlan taskContainerPlan;
        if (containerPlans.containsKey(containerId)) {
          taskContainerPlan = containerPlans.get(containerId);
          taskContainerPlan.getTaskInstances().addAll(taskInstancePlanMap.values());
        } else {
          taskContainerPlan = new ContainerPlan(
              containerId, new HashSet<>(taskInstancePlanMap.values()), containerResource);
          containerPlans.put(containerId, taskContainerPlan);
        }
      }
    }
    return new TaskSchedulePlan(0, new HashSet<>(containerPlans.values()));
  }

  private Map<Integer, List<InstanceId>> roundRobinBatchSchedulingAlgorithm(
      DataFlowTaskGraph graph, Vertex vertex) throws ScheduleException {

    Map<String, Integer> parallelTaskMap;
    if (!graph.getGraphConstraints().isEmpty()) {
      if (!graph.getNodeConstraints().isEmpty()) {
        parallelTaskMap = taskAttributes.getParallelTaskMap(vertex, graph.getNodeConstraints());
      } else {
        parallelTaskMap = taskAttributes.getParallelTaskMap(vertex);
      }
      roundRobinAllocation = attributeBasedAllocation(parallelTaskMap, graph);
    } else {
      parallelTaskMap = taskAttributes.getParallelTaskMap(vertex);
      roundRobinAllocation = nonAttributeBasedAllocation(parallelTaskMap);
    }
    return roundRobinAllocation;
  }


  private Map<Integer, List<InstanceId>> roundRobinBatchSchedulingAlgorithm(
      DataFlowTaskGraph graph, Set<Vertex> vertexSet) throws ScheduleException {

    TreeSet<Vertex> orderedTaskSet = new TreeSet<>(new VertexComparator());
    orderedTaskSet.addAll(vertexSet);

    Map<String, Integer> parallelTaskMap;
    if (!graph.getGraphConstraints().isEmpty()) {
      if (!graph.getNodeConstraints().isEmpty()) {
        parallelTaskMap = taskAttributes.getParallelTaskMap(vertexSet,
            graph.getNodeConstraints());
      } else {
        parallelTaskMap = taskAttributes.getParallelTaskMap(vertexSet);
      }
      roundRobinAllocation = attributeBasedAllocation(parallelTaskMap, graph);
    } else {
      parallelTaskMap = taskAttributes.getParallelTaskMap(vertexSet);
      roundRobinAllocation = nonAttributeBasedAllocation(parallelTaskMap);
    }
    return roundRobinAllocation;
  }

  private Map<Integer, List<InstanceId>> attributeBasedAllocation(
      Map<String, Integer> parallelTaskMap, DataFlowTaskGraph graph) {

    int containerIndex = 0;
    int instancesPerContainer = taskAttributes.getInstancesPerWorker(graph.getGraphConstraints());
    for (Map.Entry<String, Integer> e : parallelTaskMap.entrySet()) {
      String task = e.getKey();
      int taskParallelism = e.getValue();
      int numberOfInstances;
      if (instancesPerContainer < taskParallelism) {
        numberOfInstances = taskParallelism;
      } else {
        numberOfInstances = instancesPerContainer;
      }
      for (int taskIndex = 0; taskIndex < numberOfInstances; taskIndex++) {
        roundRobinAllocation.get(containerIndex).add(new InstanceId(task, gTaskId, taskIndex));
        ++containerIndex;
        if (containerIndex >= roundRobinAllocation.size()) {
          containerIndex = 0;
        }
      }
      gTaskId++;
    }
    return roundRobinAllocation;
  }

  private Map<Integer, List<InstanceId>> nonAttributeBasedAllocation(Map<String, Integer>
                                                                         parallelTaskMap) {
    int containerIndex = 0;
    for (Map.Entry<String, Integer> e : parallelTaskMap.entrySet()) {
      String task = e.getKey();
      int numberOfInstances = e.getValue();
      for (int taskIndex = 0; taskIndex < numberOfInstances; taskIndex++) {
        roundRobinAllocation.get(containerIndex).add(new InstanceId(task, gTaskId, taskIndex));
        ++containerIndex;
        if (containerIndex >= roundRobinAllocation.size()) {
          containerIndex = 0;
        }
      }
      gTaskId++;
    }
    return roundRobinAllocation;
  }

  private static class VertexComparator implements Comparator<Vertex> {
    @Override
    public int compare(Vertex o1, Vertex o2) {
      return o1.getName().compareTo(o2.getName());
    }
  }
}

