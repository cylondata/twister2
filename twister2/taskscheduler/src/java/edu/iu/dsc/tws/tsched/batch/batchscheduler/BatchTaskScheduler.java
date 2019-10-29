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
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Logger;
import java.util.stream.IntStream;

import edu.iu.dsc.tws.api.compute.exceptions.TaskSchedulerException;
import edu.iu.dsc.tws.api.compute.graph.ComputeGraph;
import edu.iu.dsc.tws.api.compute.graph.Edge;
import edu.iu.dsc.tws.api.compute.graph.Vertex;
import edu.iu.dsc.tws.api.compute.modifiers.Collector;
import edu.iu.dsc.tws.api.compute.modifiers.Receptor;
import edu.iu.dsc.tws.api.compute.nodes.INode;
import edu.iu.dsc.tws.api.compute.schedule.ITaskScheduler;
import edu.iu.dsc.tws.api.compute.schedule.elements.Resource;
import edu.iu.dsc.tws.api.compute.schedule.elements.TaskInstanceId;
import edu.iu.dsc.tws.api.compute.schedule.elements.TaskInstancePlan;
import edu.iu.dsc.tws.api.compute.schedule.elements.TaskSchedulePlan;
import edu.iu.dsc.tws.api.compute.schedule.elements.Worker;
import edu.iu.dsc.tws.api.compute.schedule.elements.WorkerPlan;
import edu.iu.dsc.tws.api.compute.schedule.elements.WorkerSchedulePlan;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;
import edu.iu.dsc.tws.tsched.spi.common.TaskSchedulerContext;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskInstanceMapCalculation;
import edu.iu.dsc.tws.tsched.utils.TaskAttributes;

/**
 * This scheduler is capable of handling of single task graph as well as multiple task graphs.
 * It first validates the parallelism of receptor and collector tasks then it invoke the schedule
 * method to schedule the individual task graphs.
 */
public class BatchTaskScheduler implements ITaskScheduler {

  private static final Logger LOG = Logger.getLogger(BatchTaskScheduler.class.getName());

  //Represents the task instance ram
  private Double instanceRAM;

  //Represents the task instance disk
  private Double instanceDisk;

  //Represents the task instance cpu value
  private Double instanceCPU;

  //Config object
  private Config config;

  //Task Attributes Object
  private TaskAttributes taskAttributes;

  private int workerId;

  private int index;

  private boolean dependentGraphs = false;

  private List<Integer> workerIdList = new ArrayList<>();

  //Batch Task Allocation Map
  private Map<Integer, List<TaskInstanceId>> batchTaskAllocation;
  private Map<String, TaskSchedulePlan> taskSchedulePlanMap = new LinkedHashMap<>();

  private static Map<String, Integer> receivableNameMap = new LinkedHashMap<>();
  private static Map<String, Integer> collectibleNameMap = new LinkedHashMap<>();

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
    this.batchTaskAllocation = new LinkedHashMap<>();
    this.taskAttributes = new TaskAttributes();
  }

  @Override
  public void initialize(Config cfg, int workerid) {
    this.initialize(cfg);
    this.workerId = workerid;
  }

  /**
   * This method receives the worker plan and the dataflow graph as a variable argument which may
   * have multiple dataflow graphs. First, it will do the validation of the parallelism values for
   * the receptor and collector tasks and then invoke the schedule() method to schedule the
   * individual task graphs.
   */
  public Map<String, TaskSchedulePlan> schedule(WorkerPlan workerPlan,
                                                ComputeGraph... computeGraphs) {
    if (computeGraphs.length > 1) {
      addReceptorsCollectors(computeGraphs);
      if (validateParallelism()) {
        dependentGraphs = true;
        for (ComputeGraph computeGraph : computeGraphs) {
          TaskSchedulePlan taskSchedulePlan = schedule(computeGraph, workerPlan);
          taskSchedulePlanMap.put(computeGraph.getGraphName(), taskSchedulePlan);
        }
      }
    } else {
      TaskSchedulePlan taskSchedulePlan = schedule(computeGraphs[0], workerPlan);
      taskSchedulePlanMap.put(computeGraphs[0].getGraphName(), taskSchedulePlan);
    }
    return taskSchedulePlanMap;
  }

  /**
   * This method is to validate the receptor task and it also has the receivable name set.
   */
  private void addReceptorsCollectors(ComputeGraph... computeGraphs) {
    for (ComputeGraph computeGraph : computeGraphs) {
      LinkedHashSet<Vertex> vertices = new LinkedHashSet<>(computeGraph.getTaskVertexSet());
      for (Vertex vertex : vertices) {
        INode iNode = vertex.getTask();
        if (iNode instanceof Receptor) {
          if (((Receptor) iNode).getReceivableNames() != null) {
            ((Receptor) iNode).getReceivableNames().forEach(
                key -> receivableNameMap.put(key, vertex.getParallelism()));
          }
        } else if (iNode instanceof Collector) {
          if (((Collector) iNode).getCollectibleNames() != null) {
            ((Collector) iNode).getCollectibleNames().forEach(
                key -> collectibleNameMap.put(key, vertex.getParallelism()));
          }
        }
      }
    }
  }

  /**
   * This is the base method which receives the dataflow taskgraph and the worker plan to allocate
   * the task instances to the appropriate workers with their required ram, disk, and cpu values.
   *
   * @param computeGraph
   * @param workerPlan   worker plan
   * @return
   */
  @Override
  public TaskSchedulePlan schedule(ComputeGraph computeGraph, WorkerPlan workerPlan) {

    //Allocate the task instances into the containers/workers
    Set<WorkerSchedulePlan> workerSchedulePlans = new LinkedHashSet<>();

    //To get the vertex set from the Collectible Name Settaskgraph
    Set<Vertex> taskVertexSet = new LinkedHashSet<>(computeGraph.getTaskVertexSet());

    //Allocate the task instances into the logical containers.
    Map<Integer, List<TaskInstanceId>> batchContainerInstanceMap =
        batchSchedulingAlgorithm(computeGraph, workerPlan.getNumberOfWorkers());

    TaskInstanceMapCalculation instanceMapCalculation = new TaskInstanceMapCalculation(
        this.instanceRAM, this.instanceCPU, this.instanceDisk);

    Map<Integer, Map<TaskInstanceId, Double>> instancesRamMap =
        instanceMapCalculation.getInstancesRamMapInContainer(batchContainerInstanceMap,
            taskVertexSet);

    Map<Integer, Map<TaskInstanceId, Double>> instancesDiskMap =
        instanceMapCalculation.getInstancesDiskMapInContainer(batchContainerInstanceMap,
            taskVertexSet);

    Map<Integer, Map<TaskInstanceId, Double>> instancesCPUMap =
        instanceMapCalculation.getInstancesCPUMapInContainer(batchContainerInstanceMap,
            taskVertexSet);

    for (int containerId : batchContainerInstanceMap.keySet()) {

      double containerRAMValue = TaskSchedulerContext.containerRamPadding(config);
      double containerDiskValue = TaskSchedulerContext.containerDiskPadding(config);
      double containerCpuValue = TaskSchedulerContext.containerCpuPadding(config);

      List<TaskInstanceId> taskTaskInstanceIds = batchContainerInstanceMap.get(containerId);
      Map<TaskInstanceId, TaskInstancePlan> taskInstancePlanMap = new HashMap<>();

      for (TaskInstanceId id : taskTaskInstanceIds) {

        double instanceRAMValue = instancesRamMap.get(containerId).get(id);
        double instanceDiskValue = instancesDiskMap.get(containerId).get(id);
        double instanceCPUValue = instancesCPUMap.get(containerId).get(id);

        Resource instanceResource = new Resource(instanceRAMValue,
            instanceDiskValue, instanceCPUValue);

        taskInstancePlanMap.put(id, new TaskInstancePlan(id.getTaskName(),
            id.getTaskId(), id.getTaskIndex(), instanceResource));

        containerRAMValue += instanceRAMValue;
        containerDiskValue += instanceDiskValue;
        containerCpuValue += instanceDiskValue;
      }

      Worker worker = workerPlan.getWorker(containerId);
      Resource containerResource;

      //Create the container resource value based on the worker plan
      if (worker != null && worker.getCpu() > 0 && worker.getDisk() > 0 && worker.getRam() > 0) {
        containerResource = new Resource((double) worker.getRam(), (double) worker.getDisk(),
            (double) worker.getCpu());
      } else {
        containerResource = new Resource(containerRAMValue, containerDiskValue, containerCpuValue);
      }

      //Schedule the task instance plan into the task container plan.
      WorkerSchedulePlan taskWorkerSchedulePlan = new WorkerSchedulePlan(containerId,
          new LinkedHashSet<>(taskInstancePlanMap.values()), containerResource);
      workerSchedulePlans.add(taskWorkerSchedulePlan);

      if (dependentGraphs && index == 0) {
        workerIdList.add(containerId);
      }
    }
    index++;
    TaskSchedulePlan taskSchedulePlan = new TaskSchedulePlan(0, workerSchedulePlans);
    if (workerId == 0) {
      Map<Integer, WorkerSchedulePlan> containersMap = taskSchedulePlan.getContainersMap();
      for (Map.Entry<Integer, WorkerSchedulePlan> entry : containersMap.entrySet()) {
        Integer integer = entry.getKey();
        WorkerSchedulePlan workerSchedulePlan = entry.getValue();
        Set<TaskInstancePlan> containerPlanTaskInstances = workerSchedulePlan.getTaskInstances();
        LOG.fine("Graph Name:" + computeGraph.getGraphName() + "\tcontainer id:" + integer);
        for (TaskInstancePlan ip : containerPlanTaskInstances) {
          LOG.fine("Task Id:" + ip.getTaskId() + "\tIndex" + ip.getTaskIndex()
              + "\tName:" + ip.getTaskName());
        }
      }
    }
    return taskSchedulePlan;
  }

  private Map<Integer, List<TaskInstanceId>> batchSchedulingAlgorithm(
      ComputeGraph graph, int numberOfContainers) throws TaskSchedulerException {

    Set<Vertex> taskVertexSet = new LinkedHashSet<>(graph.getTaskVertexSet());
    TreeSet<Vertex> orderedTaskSet = new TreeSet<>(new VertexComparator());
    orderedTaskSet.addAll(taskVertexSet);

    IntStream.range(0, numberOfContainers).forEach(
        i1 -> batchTaskAllocation.put(i1, new ArrayList<>()));

    int globalTaskIndex = 0;
    if (dependentGraphs) {
      for (Vertex vertex : taskVertexSet) {
        INode iNode = vertex.getTask();
        if (iNode instanceof Receptor) {
          validateReceptor(graph, vertex);
        }
        dependentTaskWorkerAllocation(graph, vertex, numberOfContainers, globalTaskIndex);
        globalTaskIndex++;
      }
    } else {
      for (Vertex vertex : taskVertexSet) {
        INode iNode = vertex.getTask();
        if (iNode instanceof Collector) {
          ((Collector) iNode).getCollectibleNames().forEach(
              key -> collectibleNameMap.put(key, vertex.getParallelism()));
        } else if (iNode instanceof Receptor) {
          ((Receptor) iNode).getReceivableNames().forEach(
              key -> receivableNameMap.put(key, vertex.getParallelism()));
          validateParallelism();
        }
        independentTaskWorkerAllocation(graph, vertex, numberOfContainers, globalTaskIndex);
        globalTaskIndex++;
      }
    }
    return batchTaskAllocation;
  }

  /**
   * This method is to validate the receptor and collector task in the dataflow task graphs.
   */
  private boolean validateParallelism() {
    for (Map.Entry<String, Integer> collectible : collectibleNameMap.entrySet()) {
      if (receivableNameMap.containsKey(collectible.getKey())) {
        int receptorParallel = receivableNameMap.get(collectible.getKey());
        if (receptorParallel != collectible.getValue()) {
          throw new Twister2RuntimeException("Please verify the dependent collector(s) and"
              + " receptor(s) parallelism values which are not equal");
        }
      }
    }
    return true;
  }

  /**
   * This method is for allocating the multiple dependent task graphs. First, it stores the
   * scheduled worker list in the list for scheduling the next task graphs. It gets invoked
   * when all the task graphs are connected together.
   */
  private void dependentTaskWorkerAllocation(ComputeGraph graph, Vertex vertex,
                                             int numberOfContainers, int globalTaskIndex) {
    int totalTaskInstances;
    if (graph.getNodeConstraints().isEmpty()) {
      totalTaskInstances = taskAttributes.getTotalNumberOfInstances(vertex);
      String task = vertex.getName();
      int containerIndex;
      for (int i = 0; i < totalTaskInstances; i++) {
        if (workerIdList.size() == 0) {
          containerIndex = i % numberOfContainers;
        } else {
          containerIndex = i % workerIdList.size();
        }
        batchTaskAllocation.get(containerIndex).add(
            new TaskInstanceId(task, globalTaskIndex, i));
      }
    } else {
      totalTaskInstances = taskAttributes.getTotalNumberOfInstances(
          vertex, graph.getNodeConstraints());
      int instancesPerWorker = taskAttributes.getInstancesPerWorker(
          graph.getGraphConstraints());
      int maxTaskInstancesPerContainer = 0;
      int containerIndex;
      for (int i = 0; i < totalTaskInstances; i++) {
        if (workerIdList.size() == 0) {
          containerIndex = i % numberOfContainers;
        } else {
          containerIndex = i % workerIdList.size();
        }
        if (maxTaskInstancesPerContainer < instancesPerWorker) {
          batchTaskAllocation.get(containerIndex).
              add(new TaskInstanceId(vertex.getName(), globalTaskIndex, i));
          ++maxTaskInstancesPerContainer;
        } else {
          throw new TaskSchedulerException("Task Scheduling couldn't be possible for the present"
              + "configuration, please check the number of workers maximum instances per worker");
        }
      }
    }
  }


  /**
   * This method is to allocate the task for the individual task graph.
   *
   * @param graph
   * @param vertex
   * @param numberOfContainers
   * @param globalTaskIndex
   */
  private void independentTaskWorkerAllocation(ComputeGraph graph, Vertex vertex,
                                               int numberOfContainers, int globalTaskIndex) {
    int totalTaskInstances;
    if (!graph.getNodeConstraints().isEmpty()) {
      totalTaskInstances = taskAttributes.getTotalNumberOfInstances(
          vertex, graph.getNodeConstraints());
    } else {
      totalTaskInstances = taskAttributes.getTotalNumberOfInstances(vertex);
    }

    if (!graph.getNodeConstraints().isEmpty()) {
      int instancesPerWorker = taskAttributes.getInstancesPerWorker(graph.getGraphConstraints());
      int maxTaskInstancesPerContainer = 0;
      int containerIndex;
      for (int i = 0; i < totalTaskInstances; i++) {
        containerIndex = i % numberOfContainers;
        if (maxTaskInstancesPerContainer < instancesPerWorker) {
          batchTaskAllocation.get(containerIndex).add(
              new TaskInstanceId(vertex.getName(), globalTaskIndex, i));
          ++maxTaskInstancesPerContainer;
        } else {
          throw new TaskSchedulerException("Task Scheduling couldn't be possible for the present"
              + "configuration, please check the number of workers, "
              + "maximum instances per worker");
        }
      }
    } else {
      String task = vertex.getName();
      int containerIndex;
      for (int i = 0; i < totalTaskInstances; i++) {
        containerIndex = i % numberOfContainers;
        batchTaskAllocation.get(containerIndex).add(
            new TaskInstanceId(task, globalTaskIndex, i));
      }
    }
  }


  /**
   * It is to validate the parallelism of the receptor and collector inside the task graph.
   *
   * @param vertex
   */
  private void validateReceptor(ComputeGraph graph, Vertex vertex) {
    Set<Edge> edges = graph.outEdges(vertex);
    for (Edge e : edges) {
      Vertex child = graph.childOfTask(vertex, e.getName());
      if (child.getTask() instanceof Collector) {
        if (child.getParallelism() != vertex.getParallelism()) {
          throw new TaskSchedulerException("Specify the same parallelism for parent and child tasks"
              + " which depends on the input from the parent in" + graph.getGraphName() + " graph");
        }
      }
    }
  }

  private static class VertexComparator implements Comparator<Vertex> {
    @Override
    public int compare(Vertex o1, Vertex o2) {
      return o1.getName().compareTo(o2.getName());
    }
  }
}
