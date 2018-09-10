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
package edu.iu.dsc.tws.tsched.streaming.datalocalityaware;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
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
import edu.iu.dsc.tws.tsched.spi.taskschedule.ScheduleException;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskInstanceMapCalculation;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedulePlan;
import edu.iu.dsc.tws.tsched.utils.DataTransferTimeCalculator;
import edu.iu.dsc.tws.tsched.utils.TaskAttributes;

/**
 * The data locality aware task scheduler generate the task schedule plan based on the distance
 * calculated between the worker node and the data nodes where the input data resides. Once the
 * allocation is done, it calculates the task instance ram, disk, and cpu values and also it
 * allocates the size of the container with required ram, disk, and cpu values.
 */
public class DataLocalityStreamingTaskScheduler implements ITaskScheduler {

  private static final Logger LOG = Logger.getLogger(
                                                DataLocalityStreamingTaskScheduler.class.getName());

  //Represents task schedule plan Id
  private static int taskSchedulePlanId = 0;

  //Represents the global task index value
  private static int globalTaskIndex = 0;

  //Represents task instance ram
  private Double instanceRAM;

  //Represents task instance disk
  private Double instanceDisk;

  //Represents task instance cpu
  private Double instanceCPU;

  //Represents task config object
  private Config cfg;

  //Allocated workers list
  private static List<Integer> allocatedWorkers = new ArrayList<>();

  /**
   * This method first initialize the task instance values with default task instance ram, disk, and
   * cpu values from the task scheduler context.
   */
  @Override
  public void initialize(Config config) {
    this.cfg = config;
    this.instanceRAM = TaskSchedulerContext.taskInstanceRam(cfg);
    this.instanceDisk = TaskSchedulerContext.taskInstanceDisk(cfg);
    this.instanceCPU = TaskSchedulerContext.taskInstanceCpu(cfg);
  }

  /**
   * This is the base method for the data locality aware task scheduling for scheduling the
   * streaming task instances. It retrieves the task vertex set of the task graph and send the set
   * to the data locality aware scheduling algorithm to schedule the streaming task instances
   * which are closer to the data nodes.
   */
  @Override
  public TaskSchedulePlan schedule(DataFlowTaskGraph graph, WorkerPlan workerPlan) {

    Set<TaskSchedulePlan.ContainerPlan> containerPlans = new HashSet<>();

    //Set<Vertex> taskVertexSet = new LinkedHashSet<>(graph.getTaskVertexSet());
    Set<Vertex> taskVertexSet = graph.getTaskVertexSet();

    Map<Integer, List<InstanceId>> containerInstanceMap = dataLocalityAwareSchedulingAlgorithm(
            taskVertexSet, workerPlan.getNumberOfWorkers(), workerPlan, this.cfg);

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

      double containerRAMValue = TaskSchedulerContext.containerRamPadding(cfg);
      double containerDiskValue = TaskSchedulerContext.containerDiskPadding(cfg);
      double containerCpuValue = TaskSchedulerContext.containerCpuPadding(cfg);

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
    return new TaskSchedulePlan(taskSchedulePlanId, containerPlans);
  }

  /**
   * This method is primarily responsible for generating the container and task instance map which
   * is based on the task graph, its configuration, and the allocated worker plan.
   */
  public static Map<Integer, List<InstanceId>> dataLocalityAwareSchedulingAlgorithm(
      Set<Vertex> taskVertexSet, int numberOfContainers, WorkerPlan workerPlan, Config config) {

    TaskAttributes taskAttributes = new TaskAttributes();

    //Maximum task instances can be accommodated to the container
    int instancesPerContainer = TaskSchedulerContext.defaultTaskInstancesPerContainer(config);

    //Total container capacity
    int containerCapacity = instancesPerContainer * numberOfContainers;

    int index = 0;
    int containerIndex = 0;
    //int globalTaskIndex = 0;

    //Total task instances in the taskgraph
    int totalTask = taskAttributes.getTotalNumberOfInstances(taskVertexSet);

    //Map to hold the allocation of task instances into the containers/workers
    Map<Integer, List<InstanceId>> allocationMap = new HashMap<>();

    //To check the containers can hold all the parallel task instances.
    if (containerCapacity >= totalTask) {
      LOG.info("Task scheduling could be performed for the container capacity of "
          + containerCapacity + " and " + totalTask + " task instances");
      for (int i = 0; i < numberOfContainers; i++) {
        allocationMap.put(i, new ArrayList<>());
      }
    } else {
      throw new ScheduleException("Task scheduling couldn't be performed for the container "
          + "capacity of " + containerCapacity + " and " + totalTask + " task instances");
    }

    //Parallel Task Map for the complete task graph
    Map<String, Integer> parallelTaskMap = taskAttributes.getParallelTaskMap(taskVertexSet);
    Set<Map.Entry<String, Integer>> taskEntrySet = parallelTaskMap.entrySet();

    for (Map.Entry<String, Integer> aTaskEntrySet : taskEntrySet) {
      for (Vertex vertex : taskVertexSet) {

        if (aTaskEntrySet.getKey().equals(vertex.getName())) {

          int totalTaskInstances = vertex.getParallelism();
          int maxContainerTaskObjectSize = 0;

          List<DataTransferTimeCalculator> calList = calculationList(index, config, vertex,
                  workerPlan, allocationMap, containerIndex, instancesPerContainer);

          /* This loop allocate the task instances to the respective container, before allocation
          it will check whether the container has reached maximum task instance size */

          for (int i = 0; i < totalTaskInstances; i++) {
            containerIndex = Integer.parseInt(Collections.min(calList).getNodeName().trim());
            LOG.fine("Worker Node Allocation for task:" + vertex.getName() + "(" + i + ")"
                + "-> Worker:" + containerIndex + "->" + Collections.min(calList).getDataNode());
            if (maxContainerTaskObjectSize < instancesPerContainer) {
              allocationMap.get(containerIndex).add(new InstanceId(vertex.getName(),
                                                                   globalTaskIndex, i));
              ++maxContainerTaskObjectSize;
            } else {
              LOG.warning("Worker:" + containerIndex + "reached max task objects:"
                  + maxContainerTaskObjectSize);
            }
          }
          globalTaskIndex++;
          ++index;
        }
      }
    }

    //To print the allocation map
    for (Map.Entry<Integer, List<InstanceId>> entry : allocationMap.entrySet()) {
      Integer integer = entry.getKey();
      List<InstanceId> instanceIds = entry.getValue();
      LOG.fine("Container Index:" + integer);
      for (InstanceId instanceId : instanceIds) {
        LOG.fine("Task Details:" + "\tTask Name:" + instanceId.getTaskName()
            + "\tTask id:" + instanceId.getTaskId() + "\tTask index:" + instanceId.getTaskIndex());
      }
    }
    return allocationMap;
  }

  private static List<DataTransferTimeCalculator> calculationList(int index, Config config,
                                                                  Vertex vertex,
                                                                  WorkerPlan workerPlan,
                                                                  Map<Integer,
                                                                      List<InstanceId>> aMap,
                                                                  int containerIndex,
                                                                  int maxTaskInstPerContainer) {

    DataNodeLocatorUtils dataNodeLocatorUtils = new DataNodeLocatorUtils(config);

    Map<String, List<DataTransferTimeCalculator>> workerPlanMap;

    List<String> inputDataList = vertex.getConfig().getListValue("inputdataset");
    List<DataTransferTimeCalculator> dataTransferTimeCalculatorList = null;
    List<String> datanodesList;

    /*If the index is zero, simply calculate the distance between the worker node and the
    datanodes. Else, if the index values is greater than 0, check the container has reached
    the maximum task instances per container. If it is yes, then calculationList the container to
    the allocatedWorkers list which will not be considered for the next scheduling cycle.*/

    if (inputDataList.size() > 0) {
      if (index == 0) {
        //If the vertex has input dataset and get the datanode name of the dataset in the HDFS.
        datanodesList = dataNodeLocatorUtils.findDataNodesLocation(inputDataList);
        workerPlanMap = distanceCalculation(datanodesList, workerPlan, index, allocatedWorkers);
        dataTransferTimeCalculatorList = findBestWorkerNode(vertex, workerPlanMap);

      } else if (index > 0) {

        //If the vertex has input dataset and get the datanode name of the dataset in the HDFS.
        datanodesList = dataNodeLocatorUtils.findDataNodesLocation(inputDataList);
        Worker worker = workerPlan.getWorker(containerIndex);
        if (aMap.get(containerIndex).size() >= maxTaskInstPerContainer) {
          allocatedWorkers.add(worker.getId());
        }
        workerPlanMap = distanceCalculation(datanodesList, workerPlan, index, allocatedWorkers);
        dataTransferTimeCalculatorList = findBestWorkerNode(vertex, workerPlanMap);
      }
    } else {
      throw new NullPointerException("Input Data List Is Empty");
    }

    return dataTransferTimeCalculatorList;
  }

  /**
   * It calculates the distance between the data nodes and the worker node which is based on
   * the available bandwidth, latency, and the file size.
   */
  private static Map<String, List<DataTransferTimeCalculator>> distanceCalculation(
      List<String> datanodesList, WorkerPlan workers, int index,
      List<Integer> assignedWorkers) {

    Map<String, List<DataTransferTimeCalculator>> workerPlanMap = new HashMap<>();
    Worker worker;

    //Workernode network parameters
    double workerBandwidth;
    double workerLatency;

    //Datanode network parameters
    double datanodeBandwidth;
    double datanodeLatency;

    //distance between datanode and worker node
    double calculateDistance = 0.0;

    if (index == 0) {
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
          //distanceCalculation = File Size / Bandwidth;

          calculateDataTransferTime.setRequiredDataTransferTime(calculateDistance);
          calculateDataTransferTime.setNodeName(worker.getId() + "");
          calculateDataTransferTime.setTaskIndex(index);
          calculatedVal.add(calculateDataTransferTime);
        }
        workerPlanMap.put(nodesList, calculatedVal);
      }
    } else {
      for (String nodesList : datanodesList) {
        ArrayList<DataTransferTimeCalculator> calculatedVal = new ArrayList<>();
        for (int i = 0; i < workers.getNumberOfWorkers(); i++) {
          worker = workers.getWorker(i);

          DataTransferTimeCalculator calculateDataTransferTime =
              new DataTransferTimeCalculator(nodesList, calculateDistance);

          if (!assignedWorkers.contains(worker.getId())) {

            if (worker.getProperty("bandwidth") != null && worker.getProperty("latency") != null) {
              workerBandwidth = (double) worker.getProperty("bandwidth");
              workerLatency = (double) worker.getProperty("latency");
            } else {
              workerBandwidth = TaskSchedulerContext.TWISTER2_CONTAINER_INSTANCE_BANDWIDTH_DEFAULT;
              workerLatency = TaskSchedulerContext.TWISTER2_CONTAINER_INSTANCE_LATENCY_DEFAULT;
            }

            //Right now using the default configuration values
            datanodeBandwidth = TaskSchedulerContext.TWISTER2_DATANODE_INSTANCE_BANDWIDTH_DEFAULT;
            datanodeLatency = TaskSchedulerContext.TWISTER2_DATANODE_INSTANCE_LATENCY_DEFAULT;

            //Calculate the distance between worker nodes and data nodes.
            calculateDistance = Math.abs((2 * workerBandwidth * workerLatency)
                - (2 * datanodeBandwidth * datanodeLatency));

            //(use this formula to calculate the data transfer time)
            //distanceCalculation = File Size / Bandwidth;

            calculateDataTransferTime.setRequiredDataTransferTime(calculateDistance);
            calculateDataTransferTime.setNodeName(worker.getId() + "");
            calculateDataTransferTime.setTaskIndex(index);
            calculatedVal.add(calculateDataTransferTime);
          }
        }
        workerPlanMap.put(nodesList, calculatedVal);
      }
    }
    return workerPlanMap;
  }

  /**
   * This method chooses the data node which takes minimal data transfer time.
   */
  private static List<DataTransferTimeCalculator> findBestWorkerNode(Vertex vertex, Map<String,
      List<DataTransferTimeCalculator>> workerPlanMap) {

    Set<Map.Entry<String, List<DataTransferTimeCalculator>>> entries = workerPlanMap.entrySet();
    List<DataTransferTimeCalculator> cal = new ArrayList<>();

    try {
      for (Map.Entry<String, List<DataTransferTimeCalculator>> entry : entries) {
        String key = entry.getKey();
        List<DataTransferTimeCalculator> value = entry.getValue();
        cal.add(new DataTransferTimeCalculator(Collections.min(value).getNodeName(),
            Collections.min(value).getRequiredDataTransferTime(), key));

        for (DataTransferTimeCalculator requiredDataTransferTime : entry.getValue()) {
          LOG.fine("Task:" + vertex.getName()
              + "(" + requiredDataTransferTime.getTaskIndex() + ")"
              + entry.getKey() + "D.Node:" + "-> W.Node:" + requiredDataTransferTime.getNodeName()
              + "-> D.Time:" + requiredDataTransferTime.getRequiredDataTransferTime());
        }
      }
    } catch (java.util.NoSuchElementException nse) {
      nse.printStackTrace();
    }
    return cal;
  }


  private static Map<Integer, List<InstanceId>> allocate(List<DataTransferTimeCalculator> calList,
                                                         int maxTaskInstancesPerContainer,
                                                         Map<Integer, List<InstanceId>>
                                                                 dataLocalityAwareAllocationMap,
                                                         Vertex vertex) {
    int containerIndex;
    int maxContainerTaskObjectSize = 0;
    int totalTaskInstances = vertex.getParallelism();

    try {
      for (int i = 0; i < totalTaskInstances; i++) {

        containerIndex = Integer.parseInt(Collections.min(calList).getNodeName().trim());
        LOG.fine("Worker Node Allocation for task:" + vertex.getName()
                + "(" + containerIndex + ")" + "-> Worker:" + containerIndex + "->"
                + Collections.min(calList).getDataNode());
        if (maxContainerTaskObjectSize < maxTaskInstancesPerContainer) {
          dataLocalityAwareAllocationMap.get(containerIndex).add(new InstanceId(
                  vertex.getName(), globalTaskIndex, containerIndex));
          ++maxContainerTaskObjectSize;
        } else {
          LOG.warning("Worker:" + containerIndex
                  + "reached max tasks:" + maxContainerTaskObjectSize);
        }
      }
    } catch (NoSuchElementException nse) {
      nse.printStackTrace();
    }
    return dataLocalityAwareAllocationMap;
  }
}
