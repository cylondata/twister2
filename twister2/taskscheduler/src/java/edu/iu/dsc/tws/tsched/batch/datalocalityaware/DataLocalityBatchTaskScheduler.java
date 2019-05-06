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

import java.io.IOException;
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
import edu.iu.dsc.tws.common.config.Context;
import edu.iu.dsc.tws.data.fs.FileStatus;
import edu.iu.dsc.tws.data.fs.FileSystem;
import edu.iu.dsc.tws.data.fs.Path;
import edu.iu.dsc.tws.data.utils.DataNodeLocatorUtils;
import edu.iu.dsc.tws.data.utils.DataObjectConstants;
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
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskInstanceMapCalculation;
import edu.iu.dsc.tws.tsched.spi.taskschedule.TaskSchedulePlan;
import edu.iu.dsc.tws.tsched.utils.DataTransferTimeCalculator;
import edu.iu.dsc.tws.tsched.utils.TaskAttributes;
import edu.iu.dsc.tws.tsched.utils.TaskVertexParser;

/**
 * The data locality batch task scheduler generate the task schedule plan based on the distance
 * calculated between the worker node and the data nodes where the input data resides. Once the
 * allocation is done, it calculates the task instance ram, disk, and cpu values and
 * allocates the size of the container with required ram, disk, and cpu values.
 */
public class DataLocalityBatchTaskScheduler implements ITaskScheduler {

  private static final Logger LOG
      = Logger.getLogger(DataLocalityBatchTaskScheduler.class.getName());

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

  //Data Aware Allocation Map
  private Map<Integer, List<InstanceId>> dataLocalityAwareAllocation;

  //Task Attributes Object
  private TaskAttributes taskAttributes;

  //DataNode Locator Utils Object
  private DataNodeLocatorUtils dataNodeLocatorUtils;

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
    this.dataNodeLocatorUtils = new DataNodeLocatorUtils(config);
    this.dataLocalityAwareAllocation = new HashMap<>();
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

    LinkedHashMap<Integer, ContainerPlan> containerPlans = new LinkedHashMap<>();
    for (int i = 0; i < workerPlan.getNumberOfWorkers(); i++) {
      dataLocalityAwareAllocation.put(i, new ArrayList<>());
    }

    LinkedHashSet<Vertex> taskVertexSet = new LinkedHashSet<>(graph.getTaskVertexSet());
    TaskVertexParser taskVertexParser = new TaskVertexParser();
    List<Set<Vertex>> taskVertexList = taskVertexParser.parseVertexSet(graph);

    for (Set<Vertex> vertexSet : taskVertexList) {

      Map<Integer, List<InstanceId>> containerInstanceMap;

      if (vertexSet.size() > 1) {
        containerInstanceMap = dataLocalityBatchSchedulingAlgorithm(graph, vertexSet, workerPlan);
      } else {
        Vertex vertex = vertexSet.iterator().next();
        containerInstanceMap = dataLocalityBatchSchedulingAlgorithm(graph, vertex, workerPlan);
      }

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

        if (worker != null && worker.getCpu() > 0 && worker.getDisk() > 0 && worker.getRam() > 0) {
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
          taskContainerPlan = new ContainerPlan(containerId, new HashSet<>(
              taskInstancePlanMap.values()), containerResource);
          containerPlans.put(containerId, taskContainerPlan);
        }
      }
    }

    TaskSchedulePlan taskSchedulePlan = new TaskSchedulePlan(0,
        new HashSet<>(containerPlans.values()));
    return taskSchedulePlan;
  }


  /**
   * This method is primarily responsible for generating the container and task instance map which
   * is based on the task graph, its configuration, and the allocated worker plan.
   */
  private Map<Integer, List<InstanceId>> dataLocalityBatchSchedulingAlgorithm(
      DataFlowTaskGraph graph, Vertex vertex, WorkerPlan workerPlan) {

    Map<String, Integer> parallelTaskMap;
    if (!graph.getGraphConstraints().isEmpty()) {
      if (!graph.getNodeConstraints().isEmpty()) {
        parallelTaskMap = taskAttributes.getParallelTaskMap(vertex, graph.getNodeConstraints());
      } else {
        parallelTaskMap = taskAttributes.getParallelTaskMap(vertex);
      }
      dataLocalityAwareAllocation = attributeBasedAllocation(parallelTaskMap, graph, workerPlan);
    } else {
      parallelTaskMap = taskAttributes.getParallelTaskMap(vertex);
      dataLocalityAwareAllocation = nonAttributeBasedAllocation(parallelTaskMap, workerPlan);
    }
    return dataLocalityAwareAllocation;
  }

  /**
   * This method generates the container and task instance map which is based on the task graph,
   * its configuration, and the allocated worker plan.
   *
   * @return Map
   */
  private Map<Integer, List<InstanceId>> dataLocalityBatchSchedulingAlgorithm(
      DataFlowTaskGraph graph, Set<Vertex> vertexSet, WorkerPlan workerPlan) {

    Map<String, Integer> parallelTaskMap;
    if (!graph.getGraphConstraints().isEmpty()) {
      if (!graph.getNodeConstraints().isEmpty()) {
        parallelTaskMap = taskAttributes.getParallelTaskMap(vertexSet, graph.getNodeConstraints());
      } else {
        parallelTaskMap = taskAttributes.getParallelTaskMap(vertexSet);
      }
      dataLocalityAwareAllocation = attributeBasedAllocation(parallelTaskMap, graph, workerPlan);
    } else {
      parallelTaskMap = taskAttributes.getParallelTaskMap(vertexSet);
      dataLocalityAwareAllocation = nonAttributeBasedAllocation(parallelTaskMap, workerPlan);
    }
    return dataLocalityAwareAllocation;
  }


  private Map<Integer, List<InstanceId>> attributeBasedAllocation(
      Map<String, Integer> parallelTaskMap, DataFlowTaskGraph graph, WorkerPlan workerPlan) {

    List<DataTransferTimeCalculator> workerNodeList = getWorkerNodeList(workerPlan);
    int containerIndex = Integer.parseInt(workerNodeList.get(0).getNodeName());
    int instancesPerContainer = taskAttributes.getInstancesPerWorker(graph.getGraphConstraints());

    for (Map.Entry<String, Integer> e : parallelTaskMap.entrySet()) {
      String task = e.getKey();
      int taskParallelism = e.getValue();
      for (int taskIndex = 0; taskIndex < taskParallelism; taskIndex++) {
        if (taskIndex < instancesPerContainer) {
          dataLocalityAwareAllocation.get(containerIndex).add(
              new InstanceId(task, gTaskId, taskIndex));
        } else {
          dataLocalityAwareAllocation.get(containerIndex).add(
              new InstanceId(task, gTaskId, taskIndex));
          containerIndex++;
        }
      }
      containerIndex = 0;
      gTaskId++;
    }
    return dataLocalityAwareAllocation;
  }

  private Map<Integer, List<InstanceId>> nonAttributeBasedAllocation(
      Map<String, Integer> parallelTaskMap, WorkerPlan workerPlan) {

    List<DataTransferTimeCalculator> workerNodeList = getWorkerNodeList(workerPlan);
    int instancesPerContainer = TaskSchedulerContext.defaultTaskInstancesPerContainer(config);
    int containerIndex = Integer.parseInt(workerNodeList.get(0).getNodeName());

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
        dataLocalityAwareAllocation.get(containerIndex).add(
            new InstanceId(task, gTaskId, taskIndex));
        ++containerIndex;
        if (containerIndex >= workerNodeList.size()) {
          containerIndex = 0;
        }
      }
      gTaskId++;
    }
    return dataLocalityAwareAllocation;
  }

  private List<DataTransferTimeCalculator> getWorkerNodeList(WorkerPlan workerPlan) {
    int index = 0;
    Map<String, List<DataTransferTimeCalculator>> workerPlanMap;
    List<String> inputDataList = getInputFilesList();
    List<String> datanodesList = dataNodeLocatorUtils.findDataNodesLocation(inputDataList);
    workerPlanMap = calculateDistance(datanodesList, workerPlan, index);
    List<DataTransferTimeCalculator> workerNodeList = findBestWorkerNode(workerPlanMap);
    return workerNodeList;
  }

  private List<String> getInputFilesList() {

    List<String> inputDataList = new ArrayList<>();
    String directory = null;

    if (config.get(DataObjectConstants.DINPUT_DIRECTORY) != null) {
      directory = String.valueOf(config.get(DataObjectConstants.DINPUT_DIRECTORY));
    }

    final Path path = new Path(directory + workerId);
    final FileSystem fileSystem;
    try {
      fileSystem = path.getFileSystem(config);
      if (config.get(DataObjectConstants.FILE_SYSTEM).equals(Context.TWISTER2_HDFS_FILESYSTEM)) {

        final FileStatus pathFile = fileSystem.getFileStatus(path);
        inputDataList.add(String.valueOf(pathFile.getPath()));

      } else if (config.get(DataObjectConstants.FILE_SYSTEM).equals(
          Context.TWISTER2_LOCAL_FILESYSTEM)) {

        for (FileStatus file : fileSystem.listFiles(path)) {
          String filename = String.valueOf(file.getPath());
          if (filename != null) {
            inputDataList.add(filename);
          }
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("IOException Occured");
    }
    return inputDataList;
  }

  /**
   * It calculates the distance between the data nodes and the worker nodes.
   */
  private Map<String, List<DataTransferTimeCalculator>> calculateDistance(
      List<String> datanodesList, WorkerPlan workerPlan, int taskIndex) {

    Map<String, List<DataTransferTimeCalculator>> workerPlanMap = new HashMap<>();
    Worker worker;
    double workerBandwidth;
    double workerLatency;
    double calculateDistance = 0.0;
    double datanodeBandwidth;
    double datanodeLatency;

    for (String nodesList : datanodesList) {
      ArrayList<DataTransferTimeCalculator> calculatedVal = new ArrayList<>();
      for (int i = 0; i < workerPlan.getNumberOfWorkers(); i++) {
        worker = workerPlan.getWorker(i);

        DataTransferTimeCalculator calculateDataTransferTime =
            new DataTransferTimeCalculator(nodesList, calculateDistance);

        if (worker.getProperty(Context.TWISTER2_BANDWIDTH) != null
            && worker.getProperty(Context.TWISTER2_LATENCY) != null) {
          workerBandwidth = (double) worker.getProperty(Context.TWISTER2_BANDWIDTH);
          workerLatency = (double) worker.getProperty(Context.TWISTER2_LATENCY);
        } else {
          workerBandwidth = TaskSchedulerContext.containerInstanceBandwidth(config);
          workerLatency = TaskSchedulerContext.containerInstanceLatency(config);
        }

        //Right now using the default configuration values
        datanodeBandwidth = TaskSchedulerContext.datanodeInstanceBandwidth(config);
        datanodeLatency = TaskSchedulerContext.datanodeInstanceLatency(config);

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
  private static List<DataTransferTimeCalculator> findBestWorkerNode(Map<String,
      List<DataTransferTimeCalculator>> workerPlanMap) {

    List<DataTransferTimeCalculator> cal = new ArrayList<>();
    for (Map.Entry<String, List<DataTransferTimeCalculator>> entry : workerPlanMap.entrySet()) {
      String key = entry.getKey();
      List<DataTransferTimeCalculator> value = entry.getValue();
      for (DataTransferTimeCalculator aValue : value) {
        cal.add(new DataTransferTimeCalculator(aValue.getNodeName(),
            aValue.getRequiredDataTransferTime(), key));
      }
    }
    return cal;
  }
}
