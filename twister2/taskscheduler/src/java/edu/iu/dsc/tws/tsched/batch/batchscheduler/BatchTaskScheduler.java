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

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.task.exceptions.ScheduleException;
import edu.iu.dsc.tws.api.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.api.task.graph.Edge;
import edu.iu.dsc.tws.api.task.graph.Vertex;
import edu.iu.dsc.tws.api.task.modifiers.Collector;
import edu.iu.dsc.tws.api.task.modifiers.Receptor;
import edu.iu.dsc.tws.api.task.nodes.INode;
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
import edu.iu.dsc.tws.tsched.utils.TaskAttributes;

//import java.util.stream.IntStream;
//import edu.iu.dsc.tws.api.task.modifiers.Receptor;

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

  //Task Attributes Object
  private TaskAttributes taskAttributes;

  private int workerId;

  private int index;

  private boolean dependentGraphs = false;

  //Batch Task Allocation Map
  private List<Integer> workerIdList = new ArrayList<>();
  private Map<Integer, List<TaskInstanceId>> batchTaskAllocation;
  private Map<String, List<Integer>> graphWorkerIdsMap = new LinkedHashMap<>();
  private Map<String, TaskSchedulePlan> taskSchedulePlanMap = new LinkedHashMap<>();

  private int parallelism = 0;
  private Set<String> receivableNameSet = null;

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
   * This is the base method for the data locality aware task scheduling for scheduling the batch
   * task instances. It retrieves the task vertex set of the task graph and send the set to the
   * data locality aware scheduling algorithm to allocate the batch task instances which are closer
   * to the data nodes.
   */
  public Map<String, TaskSchedulePlan> schedule(WorkerPlan workerPlan,
                                                DataFlowTaskGraph... dataFlowTaskGraph) {
    if (dataFlowTaskGraph.length > 1) {
      if (validateDependentGraphs(dataFlowTaskGraph)) {
        dependentGraphs = true;
      }
    }

    if (dependentGraphs) {
      for (DataFlowTaskGraph aDataFlowTaskGraph : dataFlowTaskGraph) {
        TaskSchedulePlan taskSchedulePlan = schedule(aDataFlowTaskGraph, workerPlan);
        if (taskSchedulePlan != null) {
          taskSchedulePlanMap.put(aDataFlowTaskGraph.getGraphName(), taskSchedulePlan);
        }
      }
    }
    return taskSchedulePlanMap;
  }

  private boolean validateDependentGraphsParallelism(DataFlowTaskGraph... dataFlowTaskGraph) {
    boolean flag = false;
    parallelism = dataFlowTaskGraph[0].getTaskVertexSet().iterator().next().getParallelism();
    for (int i = 1; i < dataFlowTaskGraph.length; i++) {
      Set<Vertex> taskVertexSet = new LinkedHashSet<>(dataFlowTaskGraph[i].getTaskVertexSet());
      if (parallelism != taskVertexSet.iterator().next().getParallelism()) {
        throw new RuntimeException("Specify the same parallelism value for the dependent graphs");
      } else {
        flag = true;
      }
    }
    return flag;
  }

  private boolean validateDependentGraphs(DataFlowTaskGraph... dataFlowTaskGraph) {
    boolean flag = false;
    int length = dataFlowTaskGraph.length;
    if (receptorTaskValidation(dataFlowTaskGraph[length - 1])
        && collectorTaskValidation(dataFlowTaskGraph)) {
      flag = true;
    }
    return flag;
  }


  private boolean receptorTaskValidation(DataFlowTaskGraph dataFlowTaskGraph) {
    Set<Vertex> childTaskVertexSet = new LinkedHashSet<>(dataFlowTaskGraph.getTaskVertexSet());
    for (Vertex vertex : childTaskVertexSet) {
      parallelism = vertex.getParallelism();
      INode iNode = vertex.getTask();
      if (iNode instanceof Receptor) {
        if (((Receptor) iNode).getReceivableNames() != null) {
          receivableNameSet = ((Receptor) iNode).getReceivableNames();
        }
      }
    }
    return true;
  }

  private boolean collectorTaskValidation(DataFlowTaskGraph... dataFlowTaskGraph) {
    boolean flag = false;
    int length = dataFlowTaskGraph.length;
    int bound = length - 1;
    for (int i = 0; i < bound; i++) {
      Set<Vertex> taskVertexSet = new LinkedHashSet<>(dataFlowTaskGraph[i].getTaskVertexSet());
      for (Vertex vertex : taskVertexSet) {
        INode iNode = vertex.getTask();
        if (iNode instanceof Collector) {
          int collectorParallelism = vertex.getParallelism();
          if (((Collector) iNode).getCollectibleNames() != null) {
            Set<String> collectibleNameSet = ((Collector) iNode).getCollectibleNames();
            if (receivableNameSet.containsAll(collectibleNameSet)) {
              if (parallelism != collectorParallelism) {
                throw new RuntimeException("Specify the same parallelism value for "
                    + "the dependent task in the task graphs");
              } else {
                flag = true;
              }
            }
          }
        }
      }
    }
    return flag;
  }

  /**
   * This is the base method which receives the dataflow taskgraph and the worker plan to allocate
   * the task instances to the appropriate workers with their required ram, disk, and cpu values.
   *
   * @return TaskSchedulePlan
   */
  @Override
  public TaskSchedulePlan schedule(DataFlowTaskGraph dataFlowTaskGraph, WorkerPlan workerPlan) {

    //Allocate the task instances into the containers/workers
    Set<WorkerSchedulePlan> workerSchedulePlans = new LinkedHashSet<>();

    //To get the vertex set from the Collectible Name Settaskgraph
    Set<Vertex> taskVertexSet = new LinkedHashSet<>(dataFlowTaskGraph.getTaskVertexSet());

    //Allocate the task instances into the logical containers.
    Map<Integer, List<TaskInstanceId>> batchContainerInstanceMap =
        batchSchedulingAlgorithm(dataFlowTaskGraph, workerPlan.getNumberOfWorkers());

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

      LOG.info("Task Instance Ids:" + taskTaskInstanceIds);

      Map<TaskInstanceId, TaskInstancePlan> taskInstancePlanMap = new HashMap<>();

      if (!taskTaskInstanceIds.isEmpty()) {
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

      if (index == 0) {
        workerIdList.add(containerId);
      }
    } ++index;

    TaskSchedulePlan taskSchedulePlan = new TaskSchedulePlan(0, workerSchedulePlans);
    Map<Integer, WorkerSchedulePlan> containersMap = taskSchedulePlan.getContainersMap();
    for (Map.Entry<Integer, WorkerSchedulePlan> entry : containersMap.entrySet()) {
      Integer integer = entry.getKey();
      WorkerSchedulePlan workerSchedulePlan = entry.getValue();
      Set<TaskInstancePlan> containerPlanTaskInstances = workerSchedulePlan.getTaskInstances();
      LOG.fine("Task Details for Container Id:" + dataFlowTaskGraph.getGraphName()
          + "\tcontainer id:" + integer);
      for (TaskInstancePlan ip : containerPlanTaskInstances) {
        LOG.fine("Task Id:" + ip.getTaskId()
            + "\tTask Index" + ip.getTaskIndex()
            + "\tTask Name:" + ip.getTaskName());
      }
    }

    return taskSchedulePlan;
    //return new TaskSchedulePlan(0, workerSchedulePlans);
  }

  /**
   * This method retrieves the parallel task map and the total number of task instances for the task
   * vertex set. Then, it will allocate the instances into the number of containers allocated for
   * the task in a round robin fashion.
   */
  private Map<Integer, List<TaskInstanceId>> batchSchedulingAlgorithm(
      DataFlowTaskGraph graph, int numberOfContainers) throws ScheduleException {

    Set<Vertex> taskVertexSet = new LinkedHashSet<>(graph.getTaskVertexSet());
    TreeSet<Vertex> orderedTaskSet = new TreeSet<>(new VertexComparator());
    orderedTaskSet.addAll(taskVertexSet);

    for (int i1 = 0; i1 < numberOfContainers; i1++) {
      batchTaskAllocation.put(i1, new ArrayList<>());
    }

    /*int totalTaskInstances = 0;
    for (Vertex vertex : taskVertexSet) {
      totalTaskInstances = taskAttributes.getTotalNumberOfInstances(vertex);
    }

    if (numberOfContainers > totalTaskInstances) {
      IntStream.range(0, totalTaskInstances).forEach(
          i1 -> batchTaskAllocation.put(i1, new ArrayList<>()));
    } else {
      IntStream.range(0, numberOfContainers).forEach(
          i1 -> batchTaskAllocation.put(i1, new ArrayList<>()));
    }*/

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
        independentTaskWorkerAllocation(graph, vertex, numberOfContainers, globalTaskIndex);
        globalTaskIndex++;
      }
    }
    return batchTaskAllocation;
  }

  private void dependentTaskWorkerAllocation(DataFlowTaskGraph graph, Vertex vertex,
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
      totalTaskInstances = taskAttributes.getTotalNumberOfInstances(vertex,
          graph.getNodeConstraints());
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
          batchTaskAllocation.get(containerIndex).add(new TaskInstanceId(
              vertex.getName(), globalTaskIndex, i));
          ++maxTaskInstancesPerContainer;
        } else {
          throw new ScheduleException("Task Scheduling couldn't be possible for the present"
              + "configuration, please check the number of workers, "
              + "maximum instances per worker");
        }
      }
    }
  }

  private void independentTaskWorkerAllocation(DataFlowTaskGraph graph, Vertex vertex,
                                               int numberOfContainers, int globalTaskIndex) {
    int totalTaskInstances;
    if (!graph.getNodeConstraints().isEmpty()) {
      totalTaskInstances = taskAttributes.getTotalNumberOfInstances(vertex,
          graph.getNodeConstraints());
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
          throw new ScheduleException("Task Scheduling couldn't be possible for the present"
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

  private void validateReceptor(DataFlowTaskGraph graph, Vertex vertex) {
    Set<Edge> edges = graph.outEdges(vertex);
    for (Edge e : edges) {
      Vertex child = graph.childOfTask(vertex, e.getName());
      if (child.getTask() instanceof Collector) {
        if (child.getParallelism() != vertex.getParallelism()) {
          throw new RuntimeException("Specify the same parallelism for parent and child tasks"
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
