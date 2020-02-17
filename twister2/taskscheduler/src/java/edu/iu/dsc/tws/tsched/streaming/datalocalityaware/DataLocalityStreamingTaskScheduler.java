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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.compute.exceptions.TaskSchedulerException;
import edu.iu.dsc.tws.api.compute.graph.ComputeGraph;
import edu.iu.dsc.tws.api.compute.graph.Vertex;
import edu.iu.dsc.tws.api.compute.schedule.ITaskScheduler;
import edu.iu.dsc.tws.api.compute.schedule.elements.Resource;
import edu.iu.dsc.tws.api.compute.schedule.elements.TaskInstanceId;
import edu.iu.dsc.tws.api.compute.schedule.elements.TaskInstancePlan;
import edu.iu.dsc.tws.api.compute.schedule.elements.TaskSchedulePlan;
import edu.iu.dsc.tws.api.compute.schedule.elements.Worker;
import edu.iu.dsc.tws.api.compute.schedule.elements.WorkerPlan;
import edu.iu.dsc.tws.api.compute.schedule.elements.WorkerSchedulePlan;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.config.Context;
import edu.iu.dsc.tws.api.data.FileStatus;
import edu.iu.dsc.tws.api.data.FileSystem;
import edu.iu.dsc.tws.api.data.Path;
import edu.iu.dsc.tws.data.utils.DataContext;
import edu.iu.dsc.tws.data.utils.DataNodeLocatorUtils;
import edu.iu.dsc.tws.data.utils.DataObjectConstants;
import edu.iu.dsc.tws.data.utils.FileSystemUtils;
import edu.iu.dsc.tws.tsched.spi.common.TaskSchedulerContext;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskInstanceMapCalculation;
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

  //Worker Id
  private int workerId = 0;

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

  @Override
  public void initialize(Config cfg, int workerid) {
    this.initialize(cfg);
    this.workerId = workerid;
  }

  /**
   * This is the base method for the data locality aware task scheduling for scheduling the
   * streaming task instances. It retrieves the task vertex set of the task graph and send the set
   * to the data locality aware scheduling algorithm to schedule the streaming task instances
   * which are closer to the data nodes.
   */
  @Override
  public TaskSchedulePlan schedule(ComputeGraph graph, WorkerPlan workerPlan) {

    //Represents task schedule plan Id
    int taskSchedulePlanId = 0;
    Set<WorkerSchedulePlan> workerSchedulePlans = new HashSet<>();
    Set<Vertex> taskVertexSet = graph.getTaskVertexSet();

    Map<Integer, List<TaskInstanceId>> containerInstanceMap =
        dataLocalityStreamingSchedulingAlgorithm(graph, workerPlan.getNumberOfWorkers(),
            workerPlan);

    TaskInstanceMapCalculation instanceMapCalculation = new TaskInstanceMapCalculation(
        this.instanceRAM, this.instanceCPU, this.instanceDisk);

    Map<Integer, Map<TaskInstanceId, Double>> instancesRamMap = instanceMapCalculation
        .getInstancesRamMapInContainer(containerInstanceMap, taskVertexSet);
    Map<Integer, Map<TaskInstanceId, Double>> instancesDiskMap = instanceMapCalculation
        .getInstancesDiskMapInContainer(containerInstanceMap, taskVertexSet);
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

        Resource instanceResource = new Resource(instanceRAMValue,
            instanceDiskValue, instanceCPUValue);

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

      WorkerSchedulePlan taskWorkerSchedulePlan = new WorkerSchedulePlan(containerId,
          new HashSet<>(taskInstancePlanMap.values()), containerResource);
      workerSchedulePlans.add(taskWorkerSchedulePlan);
    }
    return new TaskSchedulePlan(taskSchedulePlanId, workerSchedulePlans);
  }

  /**
   * This method is primarily responsible for generating the container and task instance map which
   * is based on the task graph, its configuration, and the allocated worker plan.
   */
  private Map<Integer, List<TaskInstanceId>> dataLocalityStreamingSchedulingAlgorithm(
      ComputeGraph graph, int numberOfContainers, WorkerPlan workerPlan) {

    TaskAttributes taskAttributes = new TaskAttributes();
    Set<Vertex> taskVertexSet = graph.getTaskVertexSet();

    //Maximum task instances can be accommodated to the container
    int instancesPerContainer;

    if (!graph.getGraphConstraints().isEmpty()) {
      instancesPerContainer = taskAttributes.getInstancesPerWorker(graph.getGraphConstraints());
    } else {
      instancesPerContainer = TaskSchedulerContext.defaultTaskInstancesPerContainer(this.config);
    }

    //Total container capacity
    int containerCapacity = instancesPerContainer * numberOfContainers;
    int localIndex = 0;
    int containerIndex = 0;
    int totalInstances;

    //Total task instances in the taskgraph
    if (!graph.getNodeConstraints().isEmpty()) {
      totalInstances = taskAttributes.getTotalNumberOfInstances(taskVertexSet,
          graph.getNodeConstraints());
    } else {
      totalInstances = taskAttributes.getTotalNumberOfInstances(taskVertexSet);
    }

    //Map to hold the allocation of task instances into the containers/workers
    Map<Integer, List<TaskInstanceId>> dataAwareAllocationMap = new HashMap<>();

    //To check the containers can hold all the parallel task instances.
    if (containerCapacity >= totalInstances) {
      LOG.info("Task scheduling could be performed for the container capacity of "
          + containerCapacity + " and " + totalInstances + " task instances");
      for (int i = 0; i < numberOfContainers; i++) {
        dataAwareAllocationMap.put(i, new ArrayList<>());
      }
    } else {
      throw new TaskSchedulerException("Task scheduling couldn't be performed for the container "
          + "capacity of " + containerCapacity + " and " + totalInstances + " task instances");
    }

    //Parallel Task Map for the complete task graph
    TreeSet<Vertex> orderedTaskSet = new TreeSet<>(new VertexComparator());
    orderedTaskSet.addAll(taskVertexSet);

    Map<String, Integer> parallelTaskMap;
    if (!graph.getNodeConstraints().isEmpty()) {
      parallelTaskMap = taskAttributes.getParallelTaskMap(taskVertexSet,
          graph.getNodeConstraints());
    } else {
      parallelTaskMap = taskAttributes.getParallelTaskMap(taskVertexSet);
    }

    /*This loop allocate the task instances to the respective container, before allocation
    it will check whether the container has reached maximum task instance size */
    for (Map.Entry<String, Integer> aTaskEntrySet : parallelTaskMap.entrySet()) {
      for (Vertex vertex : taskVertexSet) {
        if (aTaskEntrySet.getKey().equals(vertex.getName())) {

          int totalTaskInstances = vertex.getParallelism();
          int maxContainerTaskObjectSize = 0;

          List<DataTransferTimeCalculator> calList = dTTimecalculatorList(localIndex,
              workerPlan, dataAwareAllocationMap, containerIndex, instancesPerContainer);

          for (int i = 0; i < totalTaskInstances; i++) {
            containerIndex = Integer.parseInt(Collections.min(calList).getNodeName().trim());

            if (maxContainerTaskObjectSize < instancesPerContainer) {
              dataAwareAllocationMap.get(containerIndex).add(
                  new TaskInstanceId(vertex.getName(), globalTaskIndex, i));
              ++maxContainerTaskObjectSize;
            } else {
              throw new TaskSchedulerException("Task Scheduling couldn't be possible for the "
                  + "present configuration, please check the number of workers, "
                  + "maximum instances per worker");
            }
          }
          globalTaskIndex++;
          localIndex++;
        }
      }
    }
    return dataAwareAllocationMap;
  }

  private static class VertexComparator implements Comparator<Vertex> {
    @Override
    public int compare(Vertex o1, Vertex o2) {
      return o1.getName().compareTo(o2.getName());
    }
  }

  /**
   * Data transfer time calculator list
   */
  private List<DataTransferTimeCalculator> dTTimecalculatorList(int index,
                                                                WorkerPlan workerPlan,
                                                                Map<Integer,
                                                                    List<TaskInstanceId>> map,
                                                                int containerIndex,
                                                                int maxTaskPerContainer) {
    List<String> inputDataList = getInputFilesList();
    Map<String, List<DataTransferTimeCalculator>> workerDatanodeDistanceMap;
    List<DataTransferTimeCalculator> dataTransferTimeCalculatorList = null;
    List<String> datanodesList;

    /*If the index is zero, simply calculate the distance between the worker node and the
    datanodes. Else, if the index values is greater than 0, check the container has reached
    the maximum task instances per container. If it is yes, then calculationList the container to
    the allocatedWorkers list which will not be considered for the next scheduling cycle.*/

    DataNodeLocatorUtils dataNodeLocatorUtils = new DataNodeLocatorUtils(config);
    if (inputDataList.size() > 0) {
      if (index == 0) {
        datanodesList = dataNodeLocatorUtils.findDataNodesLocation(inputDataList);
        workerDatanodeDistanceMap = distanceCalculator(datanodesList, workerPlan, index,
            allocatedWorkers);
        dataTransferTimeCalculatorList = findBestWorkerNode(workerDatanodeDistanceMap);

      } else {
        datanodesList = dataNodeLocatorUtils.findDataNodesLocation(inputDataList);
        Worker worker = workerPlan.getWorker(containerIndex);

        if (map.get(containerIndex).size() >= maxTaskPerContainer) {
          allocatedWorkers.add(worker.getId());
        }
        workerDatanodeDistanceMap = distanceCalculator(datanodesList, workerPlan, index,
            allocatedWorkers);
        dataTransferTimeCalculatorList = findBestWorkerNode(workerDatanodeDistanceMap);
      }
    }
    return dataTransferTimeCalculatorList;
  }

  private List<String> getInputFilesList() {

    List<String> inputDataList = new ArrayList<>();
    String directory = null;

    if (config.get(DataObjectConstants.DINPUT_DIRECTORY) != null) {
      directory = String.valueOf(config.get(DataObjectConstants.DINPUT_DIRECTORY));
    }
    final Path path = new Path(directory);
    final FileSystem fileSystem;
    try {
      fileSystem = FileSystemUtils.get(path);
      if (config.get(DataObjectConstants.FILE_SYSTEM).equals(
          DataContext.TWISTER2_HDFS_FILESYSTEM)) {
        final FileStatus pathFile = fileSystem.getFileStatus(path);
        inputDataList.add(String.valueOf(pathFile.getPath()));
      } else if (config.get(DataObjectConstants.FILE_SYSTEM).equals(
          DataContext.TWISTER2_LOCAL_FILESYSTEM)) {
        for (FileStatus file : fileSystem.listFiles(path)) {
          String filename = String.valueOf(file.getPath());
          if (filename != null) {
            inputDataList.add(filename);
          }
        }
      }
    } catch (IOException e) {
      throw new TaskSchedulerException("Not able to get the input files", e);
    }
    return inputDataList;
  }


  /**
   * It calculates the distance between the data nodes and the worker node which is based on
   * the available bandwidth, latency, and the file size.
   */
  private Map<String, List<DataTransferTimeCalculator>> distanceCalculator(
      List<String> datanodesList, WorkerPlan workers, int index, List<Integer> assignedWorkers) {

    Map<String, List<DataTransferTimeCalculator>> workerDistanceMap = new HashMap<>();
    double calculateDistance = 0.0;
    for (String nodesList : datanodesList) {
      GetDistanceCalculation getAllocation = new GetDistanceCalculation(
          workers, index, calculateDistance, nodesList, assignedWorkers).invoke();
      calculateDistance = getAllocation.getCalculateDistance();
      ArrayList<DataTransferTimeCalculator> calculatedVal = getAllocation.getCalculatedVal();
      workerDistanceMap.put(nodesList, calculatedVal);
    }
    return workerDistanceMap;
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

        //TODO: Change "bandwidth" and "latency" into parameters
        if (index == 0) {
          if (worker.getProperty(Context.TWISTER2_BANDWIDTH) != null
              && worker.getProperty(Context.TWISTER2_LATENCY) != null) {
            workerBandwidth = (double) worker.getProperty(Context.TWISTER2_BANDWIDTH);
            workerLatency = (double) worker.getProperty(Context.TWISTER2_LATENCY);
          } else {
            workerBandwidth = TaskSchedulerContext.containerInstanceBandwidth(config);
            workerLatency = TaskSchedulerContext.containerInstanceLatency(config);
          }
          calculateDataTransferTime = getDistance(worker, workerBandwidth, workerLatency,
              datanodeBandwidth, datanodeLatency);
          calculatedVal.add(calculateDataTransferTime);
        } else {
          if (!assignedWorkers.contains(worker.getId())) {
            if (worker.getProperty(Context.TWISTER2_BANDWIDTH) != null
                && worker.getProperty(Context.TWISTER2_LATENCY) != null) {
              workerBandwidth = (double) worker.getProperty(Context.TWISTER2_BANDWIDTH);
              workerLatency = (double) worker.getProperty(Context.TWISTER2_LATENCY);
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

