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
import java.util.LinkedList;
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

//import java.util.Collections;

//import edu.iu.dsc.tws.api.dataset.DataPartition;

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
    this.batchTaskAllocation = new LinkedHashMap<>();
    this.taskAttributes = new TaskAttributes();
  }

  @Override
  public void initialize(Config cfg, int workerid) {
    this.initialize(cfg);
    //WorkerId
    int workerId = workerid;
  }

  /**
   * This is the base method for the data locality aware task scheduling for scheduling the batch
   * task instances. It retrieves the task vertex set of the task graph and send the set to the
   * data locality aware scheduling algorithm to allocate the batch task instances which are closer
   * to the data nodes.
   */
  public TaskSchedulePlan schedule(WorkerPlan workerPlan, DataFlowTaskGraph... dataFlowTaskGraph) {
    DataFlowTaskGraph graph;
    TaskSchedulePlan taskSchedulePlan = null;
    for (DataFlowTaskGraph aDataFlowTaskGraph : dataFlowTaskGraph) {
      graph = aDataFlowTaskGraph;
      taskSchedulePlan = schedule(graph, workerPlan);
    }
    return taskSchedulePlan;
  }

  private Map<DataFlowTaskGraph, TaskSchedulePlan> graphTaskSchedulePlanMap = new LinkedHashMap<>();

  public TaskSchedulePlan schedule(DataFlowTaskGraph graph, WorkerPlan workerPlan,
                                   Map<DataFlowTaskGraph, TaskSchedulePlan>
                                       schedulePlanMap) {
    this.graphTaskSchedulePlanMap = schedulePlanMap;
    return schedule(graph, workerPlan);
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
    }

    TaskSchedulePlan taskSchedulePlan = new TaskSchedulePlan(0, workerSchedulePlans);
    Map<Integer, WorkerSchedulePlan> containersMap = taskSchedulePlan.getContainersMap();
    for (Map.Entry<Integer, WorkerSchedulePlan> entry : containersMap.entrySet()) {
      Integer integer = entry.getKey();
      WorkerSchedulePlan workerSchedulePlan = entry.getValue();
      Set<TaskInstancePlan> containerPlanTaskInstances = workerSchedulePlan.getTaskInstances();
      LOG.info("Task Details for Container Id:" + integer);
      for (TaskInstancePlan ip : containerPlanTaskInstances) {
        LOG.info("Task Id:" + ip.getTaskId()
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

    for (int i1 = 0; i1 < numberOfContainers; i1++) {
      batchTaskAllocation.put(i1, new ArrayList<>());
    }

    Set<Vertex> taskVertexSet = new LinkedHashSet<>(graph.getTaskVertexSet());
    TreeSet<Vertex> orderedTaskSet = new TreeSet<>(new VertexComparator());
    orderedTaskSet.addAll(taskVertexSet);

    int globalTaskIndex = 0;
    List<Integer> workerIdList = null;

    for (Vertex vertex : taskVertexSet) {
      int totalTaskInstances;

      int instancesPerNode;
      if (!graph.getNodeConstraints().isEmpty()) {
        totalTaskInstances = taskAttributes.getTotalNumberOfInstances(vertex,
            graph.getNodeConstraints());
      } else {
        totalTaskInstances = taskAttributes.getTotalNumberOfInstances(vertex);
      }

      INode iNode = vertex.getTask();
      //For Validation
      if (iNode instanceof Receptor) {
        validate(graph, vertex);
        if (((Receptor) iNode).getReceivableNames() != null) {
          Set<String> receivableNamesSet = ((Receptor) iNode).getReceivableNames();
          LOG.fine("Vertex:" + vertex.getName() + "\tReceivable Name Set:" + receivableNamesSet);
          workerIdList = getDataNodeList(receivableNamesSet);
          allocate(vertex.getName(), totalTaskInstances, workerIdList, globalTaskIndex);
        }
      } else {
        if (!graph.getNodeConstraints().isEmpty()) {
          int instancesPerWorker
              = taskAttributes.getInstancesPerWorker(graph.getGraphConstraints());
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
      globalTaskIndex++;
    }

    return batchTaskAllocation;
  }

  private void allocate(String task, int totalTaskInstances, List<Integer> workerIdList,
                        int globalTaskIndex) {
//    int containerIndex;
//    for (int i = 0; i < totalTaskInstances; i++) {
//      containerIndex = i % workerIdList.size();
//      batchTaskAllocation.get(containerIndex).add(
//          new TaskInstanceId(task, globalTaskIndex, i));
//    }
//

    for (int i = 0; i < workerIdList.size(); i++) {
      int cdx = workerIdList.get(i);
      LOG.info("cdx value:" + cdx);
      for (int j = 0; j < totalTaskInstances; j++) {
        batchTaskAllocation.get(cdx).add(
            new TaskInstanceId(task, globalTaskIndex, i));
      }
    }

//    int instancesPerWorker = 2;
//    int maxTaskInstancesPerContainer = 0;
//    int containerIndex;
//
//    for (int i = 0; i < workerIdList.size(); i++) {
//      containerIndex = workerIdList.get(i);
//      for (int ii = 0; ii < totalTaskInstances; ii++) {
//        if (maxTaskInstancesPerContainer < instancesPerWorker) {
//          batchTaskAllocation.get(containerIndex).add(
//              new TaskInstanceId(task, globalTaskIndex, ii));
//          ++maxTaskInstancesPerContainer;
//        } else {
//          throw new ScheduleException("Task Scheduling couldn't be possible for the present"
//              + "configuration, please check the number of workers, "
//              + "maximum instances per worker");
//        }
//      }
//    }
  }

  private List<Integer> getDataNodeList(Set<String> receivablenameSet) {
    List<Integer> dataNodesList = null;
    for (Map.Entry<DataFlowTaskGraph, TaskSchedulePlan> entry
        : graphTaskSchedulePlanMap.entrySet()) {
      DataFlowTaskGraph graph = entry.getKey();
      Set<Vertex> taskVertexSet = new LinkedHashSet<>(graph.getTaskVertexSet());
      for (Vertex vertex : taskVertexSet) {
        INode iNode = vertex.getTask();
        if (iNode instanceof Collector) {
          if (((Collector) iNode).getCollectibleNames() != null) {
            Set<String> collectibleNamesSet = ((Collector) iNode).getCollectibleNames();
            LOG.info("%%%% Collectible Name Set: %%%%" + collectibleNamesSet);
            dataNodesList = processDataNodeList(receivablenameSet, collectibleNamesSet, entry);
          }
        }
      }
    }
    return dataNodesList;
  }

  private List<Integer> processDataNodeList(Set<String> receivablenameSet,
                                            Set<String> collectiblenameSet,
                                            Map.Entry<DataFlowTaskGraph, TaskSchedulePlan> entry) {
    List<Integer> dataNodesList = null;
    for (String receivablename : receivablenameSet) {
      if (collectiblenameSet.contains(receivablename)) {
        TaskSchedulePlan taskSchedulePlan = entry.getValue();
        Map<Integer, WorkerSchedulePlan> containersMap = taskSchedulePlan.getContainersMap();
        dataNodesList = getWorkerNodes(containersMap);
      }
    }
    return dataNodesList;
  }


  private List<Integer> getWorkerNodes(Map<Integer, WorkerSchedulePlan> containersMap) {
    List<Integer> workerNodeList = new LinkedList<>();
    for (Map.Entry<Integer, WorkerSchedulePlan> centry : containersMap.entrySet()) {
      Integer integer = centry.getKey();
      workerNodeList.add(integer);
    }
    return workerNodeList;
  }

  private void validate(DataFlowTaskGraph graph, Vertex vertex) {
    Set<Edge> edges = graph.outEdges(vertex);
    for (Edge e : edges) {
      Vertex child = graph.childOfTask(vertex, e.getName());
      if (child.getTask() instanceof Collector) {
        if (child.getParallelism() != vertex.getParallelism()) {
          throw new RuntimeException("Specify the same parallelism for parent and child which "
              + "depends on the input from the parent in\t" + graph.getGraphName() + "\tgraph");
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
