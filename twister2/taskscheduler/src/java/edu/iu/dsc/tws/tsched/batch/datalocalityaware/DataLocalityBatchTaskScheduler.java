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
package edu.iu.dsc.tws.tsched.batch.datalocalityaware;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.utils.DataNodeLocatorUtils;
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
import edu.iu.dsc.tws.tsched.utils.DataTransferTimeCalculator;
import edu.iu.dsc.tws.tsched.utils.TaskAttributes;
import edu.iu.dsc.tws.tsched.utils.TaskVertexParser;

/**
 * The data locality aware task scheduler generate the task schedule plan based on the distance
 * calculated between the worker node and the data nodes where the input data resides. Once the
 * allocation is done, it calculates the task instance ram, disk, and cpu values and
 * allocates the size of the container with required ram, disk, and cpu values.
 */
public class DataLocalityBatchTaskScheduler implements ITaskScheduler {

  private static final Logger LOG = Logger.getLogger(
      DataLocalityBatchTaskScheduler.class.getName());

  private static int taskSchedulePlanId = 0;
  private static int globalTaskIndex = 0;
  private Double instanceRAM;
  private Double instanceDisk;
  private Double instanceCPU;
  private Config config;

  /**
   * This method is primarily responsible for generating the container and task instance map which
   * is based on the task graph, its configuration, and the allocated worker plan.
   */
  private static Map<Integer, List<InstanceId>> dataLocalityBatchSchedulingAlgo(
      Vertex taskVertex, int numberOfContainers, WorkerPlan workerPlan, Config config) {

    DataNodeLocatorUtils dataNodeLocatorUtils = new DataNodeLocatorUtils(config);
    TaskAttributes taskAttributes = new TaskAttributes();

    int maxTaskInstancesPerContainer =
        TaskSchedulerContext.defaultTaskInstancesPerContainer(config);
    int cIdx = 0;
    int containerIndex;

    Map<String, Integer> parallelTaskMap = taskAttributes.getParallelTaskMap(taskVertex);
    Map<Integer, List<InstanceId>> dataAwareAllocation = new HashMap<>();
    Set<Map.Entry<String, Integer>> taskEntrySet = parallelTaskMap.entrySet();

    for (int i = 0; i < numberOfContainers; i++) {
      dataAwareAllocation.put(i, new ArrayList<>());
    }

    for (Map.Entry<String, Integer> aTaskEntrySet : taskEntrySet) {

      Map<String, List<DataTransferTimeCalculator>> workerPlanMap;
      String taskName = aTaskEntrySet.getKey();

      /*
      If the vertex has the input data set, get the status and the datanode name of the input
      dataset in the HDFS.
      */

      if (taskVertex.getName().equals(taskName)
          && (taskVertex.getConfig().getListValue("inputdataset") != null)) {

        int totalNumberOfInstances = taskVertex.getParallelism();
        List<DataTransferTimeCalculator> cal;
        List<String> datanodesList;

        datanodesList = dataNodeLocatorUtils.
            findDataNodesLocation(taskVertex.getConfig().getListValue("inputdataset"));
        workerPlanMap = distanceCalculation(datanodesList, workerPlan, cIdx);
        cal = findBestWorkerNode(taskVertex, workerPlanMap);


        /*
          This loop allocate the task instances to the respective container, before allocation
          it will check whether the container has reached maximum task instance size which is
          able to hold.
         */
        for (int i = 0; i < totalNumberOfInstances; i++) {
          int maxContainerTaskObjectSize = 0;
          if (maxContainerTaskObjectSize <= maxTaskInstancesPerContainer) {
            containerIndex = Integer.parseInt(cal.get(i).getNodeName().trim());
            LOG.fine("Worker Node Allocation for task:" + taskName + "(" + i + ")"
                + "-> Worker:" + containerIndex + "->" + Collections.min(cal).getDataNode());
            dataAwareAllocation.get(containerIndex).add(
                new InstanceId(taskVertex.getName(), globalTaskIndex, i));
            maxContainerTaskObjectSize++;
          }
        }
        globalTaskIndex++;
      }
    }

    for (Map.Entry<Integer, List<InstanceId>> entry : dataAwareAllocation.entrySet()) {
      Integer integer = entry.getKey();
      List<InstanceId> instanceIds = entry.getValue();
      LOG.fine("Container Index:" + integer);
      for (InstanceId instanceId : instanceIds) {
        LOG.fine("Task Instance Scheduled Details:"
            + "-> Task Name:" + instanceId.getTaskName()
            + "-> Task id:" + instanceId.getTaskId()
            + "-> Task index:" + instanceId.getTaskIndex());
      }
    }
    return dataAwareAllocation;
  }

  /**
   * This method generates the container and task instance map
   * based on the task graph, its configuration, and the allocated worker plan.
   */
  private static Map<Integer, List<InstanceId>> dataLocalityBatchSchedulingAlgo(
      Set<Vertex> taskVertexSet, int numberOfContainers, WorkerPlan workerPlan, Config config) {

    DataNodeLocatorUtils dataNodeLocatorUtils = new DataNodeLocatorUtils(config);
    TaskAttributes taskAttributes = new TaskAttributes();

    int cIdx = 0;
    int containerIndex;

    Map<String, Integer> parallelTaskMap = taskAttributes.getParallelTaskMap(taskVertexSet);
    Map<Integer, List<InstanceId>> dataAwareAllocation = new HashMap<>();
    Set<Map.Entry<String, Integer>> taskEntrySet = parallelTaskMap.entrySet();

    for (int i = 0; i < numberOfContainers; i++) {
      dataAwareAllocation.put(i, new ArrayList<>());
    }

    for (Map.Entry<String, Integer> aTaskEntrySet : taskEntrySet) {

      Map<String, List<DataTransferTimeCalculator>> workerPlanMap;
      String taskName = aTaskEntrySet.getKey();

      /*If the vertex has the input data set list, get the status and path of the file in HDFS.*/
      for (Vertex vertex : taskVertexSet) {
        if (vertex.getName().equals(taskName)
            && vertex.getConfig().getListValue("inputdataset") != null) {

          int totalNumberOfInstances = vertex.getParallelism();
          List<DataTransferTimeCalculator> cal;
          List<String> datanodesList;

          datanodesList = dataNodeLocatorUtils.
              findDataNodesLocation(vertex.getConfig().getListValue("inputdataset"));
          workerPlanMap = distanceCalculation(datanodesList, workerPlan, cIdx);
          cal = findBestWorkerNode(vertex, workerPlanMap);

          for (int i = 0; i < totalNumberOfInstances; i++) {
            containerIndex = Integer.parseInt(cal.get(i).getNodeName().trim());
            LOG.fine("Worker Node Allocation for task:" + taskName + "(" + i + ")"
                + "-> Worker:" + containerIndex + "->" + Collections.min(cal).getDataNode());
            dataAwareAllocation.get(containerIndex).add(
                new InstanceId(vertex.getName(), globalTaskIndex, i));
          }
          globalTaskIndex++;
        }
      }
    }

    for (Map.Entry<Integer, List<InstanceId>> entry1 : dataAwareAllocation.entrySet()) {
      Integer integer = entry1.getKey();
      List<InstanceId> instanceIds = entry1.getValue();
      LOG.info("Container Index:" + integer);
      for (InstanceId instanceId : instanceIds) {
        LOG.info("Task Details:"
            + "\t Task Name:" + instanceId.getTaskName()
            + "\t Task id:" + instanceId.getTaskId()
            + "\t Task index:" + instanceId.getTaskIndex());
      }
    }
    return dataAwareAllocation;
  }

  /**
   * It calculates the distance between the data nodes and the worker nodes.
   */
  private static Map<String, List<DataTransferTimeCalculator>> distanceCalculation(
      List<String> datanodesList, WorkerPlan workers, int taskIndex) {

    Map<String, List<DataTransferTimeCalculator>> workerPlanMap = new HashMap<>();
    Worker worker;
    double workerBandwidth;
    double workerLatency;
    double calculateDistance = 0.0;
    double datanodeBandwidth;
    double datanodeLatency;

    for (String nodesList : datanodesList) {
      ArrayList<DataTransferTimeCalculator> calculatedVal = new ArrayList<>();
      for (int i = 0; i < workers.getNumberOfWorkers(); i++) {
        worker = workers.getWorker(i);

        if (worker.getProperty("bandwidth") != null && worker.getProperty("latency") != null) {
          workerBandwidth = (double) worker.getProperty("bandwidth");
          workerLatency = (double) worker.getProperty("latency");
        } else {
          workerBandwidth = TaskSchedulerContext.TWISTER2_CONTAINER_INSTANCE_BANDWIDTH_DEFAULT;
          workerLatency = TaskSchedulerContext.TWISTER2_CONTAINER_INSTANCE_LATENCY_DEFAULT;
        }

        DataTransferTimeCalculator calculateDataTransferTime =
            new DataTransferTimeCalculator(nodesList, calculateDistance);

        //Right now using the default configuration values
        datanodeBandwidth = TaskSchedulerContext.TWISTER2_DATANODE_INSTANCE_BANDWIDTH_DEFAULT;
        datanodeLatency = TaskSchedulerContext.TWISTER2_DATANODE_INSTANCE_LATENCY_DEFAULT;

        //Calculate the distance between worker nodes and data nodes.
        calculateDistance = Math.abs((2 * workerBandwidth * workerLatency)
            - (2 * datanodeBandwidth * datanodeLatency));

        //(use this formula to calculate the data transfer time)
        //calculateDistance = File Size / Bandwidth;

        calculateDataTransferTime.setRequiredDataTransferTime(calculateDistance);
        calculateDataTransferTime.setNodeName(worker.getId() + "");
        calculateDataTransferTime.setTaskIndex(taskIndex);
        calculatedVal.add(calculateDataTransferTime);
      }
      workerPlanMap.put(nodesList, calculatedVal);
    }
    return workerPlanMap;
  }

  /**
   * This method finds the worker node which has better network parameters (bandwidth/latency)
   * or it will take lesser time for the data transfer if there is any.
   */
  private static List<DataTransferTimeCalculator> findBestWorkerNode(Vertex vertex, Map<String,
      List<DataTransferTimeCalculator>> workerPlanMap) {

    Set<Map.Entry<String, List<DataTransferTimeCalculator>>> entries = workerPlanMap.entrySet();
    List<DataTransferTimeCalculator> cal = new ArrayList<>();

    try {
      for (Map.Entry<String, List<DataTransferTimeCalculator>> entry : entries) {

        String key = entry.getKey();
        List<DataTransferTimeCalculator> value = entry.getValue();
        for (DataTransferTimeCalculator aValue : value) {
          cal.add(new DataTransferTimeCalculator(aValue.getNodeName(),
              aValue.getRequiredDataTransferTime(), key));
        }
        for (DataTransferTimeCalculator requiredDataTransferTime : entry.getValue()) {
          LOG.fine("Task:" + vertex.getName()
              + "(" + requiredDataTransferTime.getTaskIndex() + ")"
              + entry.getKey() + "D.Node:" + "-> W.Node:" + requiredDataTransferTime.getNodeName()
              + "-> D.Time:" + requiredDataTransferTime.getRequiredDataTransferTime());
        }
      }
    } catch (NoSuchElementException nse) {
      nse.printStackTrace();
    }
    return cal;
  }

  /**
   * This method first initialize the task instance values with default task instance ram, disk, and
   * cpu values from the task scheduler context.
   */
  @Override
  public void initialize(Config cfg) {
    this.config = cfg;
    this.instanceRAM = TaskSchedulerContext.taskInstanceRam(this.config);
    this.instanceDisk = TaskSchedulerContext.taskInstanceDisk(this.config);
    this.instanceCPU = TaskSchedulerContext.taskInstanceCpu(this.config);
  }

  /**
   * This is the base method for the data locality aware task scheduling for scheduling the batch
   * task instances. It retrieves the task vertex set of the task graph and send the set to the data
   * locality aware scheduling algorithm to allocate the batch task instances which are closer
   * to the data nodes.
   */
  public List<TaskSchedulePlan> scheduleBatch(DataFlowTaskGraph graph, WorkerPlan workerPlan) {

    Map<Integer, List<InstanceId>> containerInstanceMap;

    Set<Vertex> taskVertexSet = new LinkedHashSet<>(graph.getTaskVertexSet());

    List<TaskSchedulePlan> taskSchedulePlanList = new ArrayList<>();

    List<Set<Vertex>> taskVertexList = TaskVertexParser.parseVertexSet(graph);

    for (Set<Vertex> vertexSet : taskVertexList) {
      if (vertexSet.size() > 1) {
        containerInstanceMap = dataLocalityBatchSchedulingAlgo(vertexSet,
            workerPlan.getNumberOfWorkers(), workerPlan, this.config);
      } else {
        Vertex vertex = vertexSet.iterator().next();
        containerInstanceMap = dataLocalityBatchSchedulingAlgo(vertex,
            workerPlan.getNumberOfWorkers(), workerPlan, this.config);
      }

      Set<TaskSchedulePlan.ContainerPlan> containerPlans = new HashSet<>();

      TaskInstanceMapCalculation instanceMapCalculation = new TaskInstanceMapCalculation(
          this.instanceRAM, this.instanceCPU, this.instanceDisk);

      Map<Integer, Map<InstanceId, Double>> instancesRamMap = instanceMapCalculation.
          getInstancesRamMapInContainer(containerInstanceMap, taskVertexSet);

      Map<Integer, Map<InstanceId, Double>> instancesDiskMap = instanceMapCalculation.
          getInstancesDiskMapInContainer(containerInstanceMap, taskVertexSet);

      Map<Integer, Map<InstanceId, Double>> instancesCPUMap = instanceMapCalculation.
          getInstancesCPUMapInContainer(containerInstanceMap, taskVertexSet);

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

          Resource instanceResource = new Resource(instanceRAMValue, instanceDiskValue,
              instanceCPUValue);

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

    //To print the schedule plan list
    for (TaskSchedulePlan taskSchedulePlan : taskSchedulePlanList) {
      Map<Integer, TaskSchedulePlan.ContainerPlan> containersMap
          = taskSchedulePlan.getContainersMap();
      for (Map.Entry<Integer, TaskSchedulePlan.ContainerPlan> entry : containersMap.entrySet()) {
        Integer integer = entry.getKey();
        TaskSchedulePlan.ContainerPlan containerPlan = entry.getValue();
        Set<TaskSchedulePlan.TaskInstancePlan> taskContainerPlan = containerPlan.getTaskInstances();
        for (TaskSchedulePlan.TaskInstancePlan ip : taskContainerPlan) {
          LOG.info("Task Id:" + ip.getTaskId() + "\tTask Index" + ip.getTaskIndex()
              + "\tTask Name:" + ip.getTaskName() + "\tContainer Id:" + integer);
        }
      }
    }
    return taskSchedulePlanList;
  }

  @Override
  public TaskSchedulePlan schedule(DataFlowTaskGraph graph, WorkerPlan workerPlan) {
    return null;
  }
}
