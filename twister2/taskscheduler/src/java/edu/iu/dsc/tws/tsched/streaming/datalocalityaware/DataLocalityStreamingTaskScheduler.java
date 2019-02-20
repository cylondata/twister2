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
 * The data locality streaming task scheduler generate the task schedule plan based on the distance
 * calculated between the worker node and the data nodes where the input data resides. Once the
 * allocation is done, it calculates the task instance ram, disk, and cpu values and also it
 * allocates the size of the container with required ram, disk, and cpu values.
 */
public class DataLocalityStreamingTaskScheduler implements ITaskScheduler {

  private static final Logger LOG =
      Logger.getLogger(DataLocalityStreamingTaskScheduler.class.getName());

  //Represents the global task index value
  private static int globalTaskIndex = 0;

  //Represents task instance ram
  private Double instanceRAM;

  //Represents task instance disk
  private Double instanceDisk;

  //Represents task instance cpu
  private Double instanceCPU;

  //Represents task config object
  private Config config;

  //Allocated workers list
  private static List<Integer> allocatedWorkers = new ArrayList<>();

  /**
   * This method first initialize the task instance values with default task instance ram, disk, and
   * cpu values from the task scheduler context.
   */
  @Override
  public void initialize(Config cfg) {
    LOG.info("Config Values Are" + cfg);
    this.config = cfg;
    this.instanceRAM = TaskSchedulerContext.taskInstanceRam(this.config);
    this.instanceDisk = TaskSchedulerContext.taskInstanceDisk(this.config);
    this.instanceCPU = TaskSchedulerContext.taskInstanceCpu(this.config);
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

    Set<Vertex> taskVertexSet = graph.getTaskVertexSet();

    Map<Integer, List<InstanceId>> containerInstanceMap = dataLocalityStreamingSchedulingAlgorithm(
        taskVertexSet, workerPlan.getNumberOfWorkers(), workerPlan);

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

      if (worker != null && worker.getCpu() > 0 && worker.getDisk() > 0 && worker.getRam() > 0) {
        containerResource = new Resource((double) worker.getRam(),
            (double) worker.getDisk(), (double) worker.getCpu());
      } else {
        containerResource = new Resource(containerRAMValue, containerDiskValue,
            containerCpuValue);
      }

      TaskSchedulePlan.ContainerPlan taskContainerPlan =
          new TaskSchedulePlan.ContainerPlan(containerId,
              new HashSet<>(taskInstancePlanMap.values()), containerResource);
      containerPlans.add(taskContainerPlan);
    }
    //Represents task schedule plan Id
    int taskSchedulePlanId = 0;
    return new TaskSchedulePlan(taskSchedulePlanId, containerPlans);
  }

  /**
   * This method is primarily responsible for generating the container and task instance map which
   * is based on the task graph, its configuration, and the allocated worker plan.
   */
  private Map<Integer, List<InstanceId>> dataLocalityStreamingSchedulingAlgorithm(
      Set<Vertex> taskVertexSet, int numberOfContainers, WorkerPlan workerPlan) {

    TaskAttributes taskAttributes = new TaskAttributes();

    //Maximum task instances can be accommodated to the container
    int instancesPerContainer = TaskSchedulerContext.defaultTaskInstancesPerContainer(config);

    //Total container capacity
    int containerCapacity = instancesPerContainer * numberOfContainers;
    int localIndex = 0;
    int containerIndex = 0;

    //Total task instances in the taskgraph
    int totalTask = taskAttributes.getTotalNumberOfInstances(taskVertexSet);

    //Map to hold the allocation of task instances into the containers/workers
    Map<Integer, List<InstanceId>> dataAwareAllocationMap = new HashMap<>();

    //To check the containers can hold all the parallel task instances.
    if (containerCapacity >= totalTask) {
      LOG.info("Task scheduling could be performed for the container capacity of "
          + containerCapacity + " and " + totalTask + " task instances");
      for (int i = 0; i < numberOfContainers; i++) {
        dataAwareAllocationMap.put(i, new ArrayList<>());
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

          List<DataTransferTimeCalculator> calList = calculationList(localIndex, vertex,
              workerPlan, dataAwareAllocationMap, containerIndex, instancesPerContainer);

          /* This loop allocate the task instances to the respective container, before allocation
          it will check whether the container has reached maximum task instance size */
          for (int i = 0; i < totalTaskInstances; i++) {
            containerIndex = Integer.parseInt(Collections.min(calList).getNodeName().trim());
            if (maxContainerTaskObjectSize < instancesPerContainer) {
              dataAwareAllocationMap.get(containerIndex).add(new InstanceId(vertex.getName(),
                  globalTaskIndex, i));
              ++maxContainerTaskObjectSize;
            } else {
              LOG.warning("Worker:" + containerIndex + "reached max task objects:"
                  + maxContainerTaskObjectSize);
            }
          }
          globalTaskIndex++;
          localIndex++;
        }
      }
    }
    return dataAwareAllocationMap;
  }

  private List<DataTransferTimeCalculator> calculationList(int index, Vertex vertex,
                                                           WorkerPlan workerPlan,
                                                           Map<Integer, List<InstanceId>> aMap,
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
    if (index == 0) {
      datanodesList = dataNodeLocatorUtils.findDataNodesLocation(inputDataList);
      workerPlanMap = distanceCalculation(datanodesList, workerPlan, index, allocatedWorkers);
      dataTransferTimeCalculatorList = findBestWorkerNode(workerPlanMap);

    } else if (index > 0) {
      datanodesList = dataNodeLocatorUtils.findDataNodesLocation(inputDataList);
      Worker worker = workerPlan.getWorker(containerIndex);
      if (aMap.get(containerIndex).size() >= maxTaskInstPerContainer) {
        allocatedWorkers.add(worker.getId());
      }
      workerPlanMap = distanceCalculation(datanodesList, workerPlan, index, allocatedWorkers);
      dataTransferTimeCalculatorList = findBestWorkerNode(workerPlanMap);
    }
    return dataTransferTimeCalculatorList;
  }

  /**
   * It calculates the distance between the data nodes and the worker node which is based on
   * the available bandwidth, latency, and the file size.
   */
  private Map<String, List<DataTransferTimeCalculator>> distanceCalculation(
      List<String> datanodesList, WorkerPlan workers, int index,
      List<Integer> assignedWorkers) {

    Map<String, List<DataTransferTimeCalculator>> workerPlanMap = new HashMap<>();

    //distance between datanode and worker node
    double calculateDistance = 0.0;

    for (String nodesList : datanodesList) {
      GetDistanceCalculation getAllocation = new GetDistanceCalculation(
          workers, index, calculateDistance, nodesList, assignedWorkers).invoke();
      calculateDistance = getAllocation.getCalculateDistance();
      ArrayList<DataTransferTimeCalculator> calculatedVal = getAllocation.getCalculatedVal();
      workerPlanMap.put(nodesList, calculatedVal);
    }
    return workerPlanMap;
  }

  private class GetDistanceCalculation {

    private WorkerPlan workers;
    private int index;
    private double calculateDistance;
    private String nodesList;
    private ArrayList<DataTransferTimeCalculator> calculatedVal;
    private List<Integer> assignedWorkers;

    GetDistanceCalculation(WorkerPlan workers, int index, double calculateDistance,
                           String nodesList, List<Integer> allocatedWorkers) {
      this.workers = workers;
      this.index = index;
      this.calculateDistance = calculateDistance;
      this.nodesList = nodesList;
      this.assignedWorkers = allocatedWorkers;
    }

    private double getCalculateDistance() {
      return calculateDistance;
    }

    private ArrayList<DataTransferTimeCalculator> getCalculatedVal() {
      return calculatedVal;
    }

    private GetDistanceCalculation invoke() {

      double workerBandwidth;
      double workerLatency;
      double datanodeBandwidth;
      double datanodeLatency;

      Worker worker;
      DataTransferTimeCalculator calculateDataTransferTime;

      calculatedVal = new ArrayList<>();

      for (int i = 0; i < workers.getNumberOfWorkers(); i++) {
        worker = workers.getWorker(i);

        //Right now using the default configuration values
        datanodeBandwidth = TaskSchedulerContext.datanodeInstanceBandwidth(config);
        datanodeLatency = TaskSchedulerContext.datanodeInstanceLatency(config);

        if (index == 0) {
          if (worker.getProperty("bandwidth") != null && worker.getProperty("latency") != null) {
            workerBandwidth = (double) worker.getProperty("bandwidth");
            workerLatency = (double) worker.getProperty("latency");
          } else {
            workerBandwidth = TaskSchedulerContext.containerInstanceBandwidth(config);
            workerLatency = TaskSchedulerContext.containerInstanceLatency(config);
          }
          calculateDataTransferTime = getDistance(worker, workerBandwidth, workerLatency,
              datanodeBandwidth, datanodeLatency);
          calculatedVal.add(calculateDataTransferTime);
        } else {
          if (!assignedWorkers.contains(worker.getId())) {
            if (worker.getProperty("bandwidth") != null && worker.getProperty("latency") != null) {
              workerBandwidth = (double) worker.getProperty("bandwidth");
              workerLatency = (double) worker.getProperty("latency");
            } else {
              workerBandwidth = TaskSchedulerContext.containerInstanceBandwidth(config);
              workerLatency = TaskSchedulerContext.containerInstanceLatency(config);
            }
            calculateDataTransferTime = getDistance(worker, workerBandwidth, workerLatency,
                datanodeBandwidth, datanodeLatency);
            calculatedVal.add(calculateDataTransferTime);
          }
        }
      }
      return this;
    }

    private DataTransferTimeCalculator getDistance(Worker worker, double workerBandwidth,
                                                   double workerLatency, double datanodeBandwidth,
                                                   double datanodeLatency) {

      DataTransferTimeCalculator calculateDataTransferTime =
          new DataTransferTimeCalculator(nodesList, calculateDistance);

      //Calculate the distance between worker nodes and data nodes.
      calculateDistance = Math.abs((2 * workerBandwidth * workerLatency)
          - (2 * datanodeBandwidth * datanodeLatency));

      //(We may use this formula to calculate the data transfer time)
      //distanceCalculation = File Size / Bandwidth;

      calculateDataTransferTime.setRequiredDataTransferTime(calculateDistance);
      calculateDataTransferTime.setNodeName(worker.getId() + "");
      calculateDataTransferTime.setTaskIndex(index);
      return calculateDataTransferTime;
    }
  }

  /**
   * This method chooses the data node which takes minimal data transfer time.
   *
   * @return List
   */
  private static List<DataTransferTimeCalculator> findBestWorkerNode(Map<String,
      List<DataTransferTimeCalculator>> workerPlanMap) {

    List<DataTransferTimeCalculator> cal = new ArrayList<>();
    for (Map.Entry<String, List<DataTransferTimeCalculator>> entry : workerPlanMap.entrySet()) {
      String key = entry.getKey();
      List<DataTransferTimeCalculator> value = entry.getValue();
      cal.add(new DataTransferTimeCalculator(Collections.min(value).getNodeName(),
          Collections.min(value).getRequiredDataTransferTime(), key));
    }
    return cal;
  }
}
