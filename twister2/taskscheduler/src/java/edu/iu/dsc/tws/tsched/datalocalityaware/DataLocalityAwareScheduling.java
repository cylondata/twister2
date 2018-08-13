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
package edu.iu.dsc.tws.tsched.datalocalityaware;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.utils.DataNodeLocatorUtils;
import edu.iu.dsc.tws.task.graph.Vertex;
import edu.iu.dsc.tws.tsched.spi.common.TaskSchedulerContext;
import edu.iu.dsc.tws.tsched.spi.scheduler.Worker;
import edu.iu.dsc.tws.tsched.spi.scheduler.WorkerPlan;
import edu.iu.dsc.tws.tsched.spi.taskschedule.InstanceId;
import edu.iu.dsc.tws.tsched.spi.taskschedule.ScheduleException;
import edu.iu.dsc.tws.tsched.utils.CalculateDataTransferTime;
import edu.iu.dsc.tws.tsched.utils.TaskAttributes;

/**
 * This class is responsible for scheduling the task graph instances into the worker nodes
 * based on the locality of the data.
 */
public class DataLocalityAwareScheduling {

  private static final Logger LOG = Logger.getLogger(DataLocalityAwareScheduling.class.getName());

  protected DataLocalityAwareScheduling() {
  }

  /**
   * This method is primarily responsible for generating the container and task instance map which
   * is based on the task graph, its configuration, and the allocated worker plan.
   */
  public static Map<Integer, List<InstanceId>> DataLocalityAwareSchedulingAlgorithm(
      Set<Vertex> taskVertexSet, int numberOfContainers, WorkerPlan workerPlan, Config config) {

    DataNodeLocatorUtils dataNodeLocatorUtils = new DataNodeLocatorUtils(config);
    TaskAttributes taskAttributes = new TaskAttributes();

    int maxTaskInstancesPerContainer =
        TaskSchedulerContext.defaultTaskInstancesPerContainer(config);
    int containerCapacity = maxTaskInstancesPerContainer * numberOfContainers;
    int totalTask = taskAttributes.getTotalNumberOfInstances(taskVertexSet);
    int cIdx = 0;
    int containerIndex = 0;
    int globalTaskIndex = 0;

    Map<String, Integer> parallelTaskMap = taskAttributes.getParallelTaskMap(taskVertexSet);
    Map<Integer, List<InstanceId>> dataAwareAllocation = new HashMap<>();

    Set<Map.Entry<String, Integer>> taskEntrySet = parallelTaskMap.entrySet();
    List<Integer> allocatedWorkers = new ArrayList<>();

    LOG.fine("No. of Containers:\t" + numberOfContainers
        + "\tMax Task Instances Per Container:\t" + maxTaskInstancesPerContainer);

    //To check the allocated containers can hold all the parallel task instances.
    if (containerCapacity >= totalTask) {
      LOG.info("Task Scheduling Can be Performed for the Container Capacity of "
          + containerCapacity + " and " + totalTask + " Task Instances");
      for (int i = 0; i < numberOfContainers; i++) {
        dataAwareAllocation.put(i, new ArrayList<>());
      }
    } else {
      throw new ScheduleException("Task Scheduling "
          + "Can't be Performed for the Container Capacity of "
          + containerCapacity + " and " + totalTask + " Task Instances");
    }

    LOG.info("Data Aware Before Task Allocation:\t" + dataAwareAllocation);

    for (Map.Entry<String, Integer> aTaskEntrySet : taskEntrySet) {

      Map<String, List<CalculateDataTransferTime>> workerPlanMap;
      Map.Entry<String, Integer> entry = aTaskEntrySet;
      String taskName = entry.getKey();

      // If the vertex has input dataset and get the datanode name of the
      //dataset in the HDFS.
      for (Vertex vertex : taskVertexSet) {
        if (vertex.getName().equals(taskName)
            && vertex.getConfig().getListValue("inputdataset") != null) {

          int totalNumberOfInstances = vertex.getParallelism();
          int maxContainerTaskObjectSize = 0;

          List<CalculateDataTransferTime> cal;
          List<String> datanodesList;
          /*
            If the cIdx is zero, simply calculate the distance between the worker node and
            the datanodes. Else, if the cIdx values is greater than 0, check the container
            has reached the maximum task instances per container. If it is yes, then allocate
            the container to the allocatedWorkers list which will not be considered for the
            next scheduling cycle.
           */
          if (cIdx == 0) {
            datanodesList = dataNodeLocatorUtils.
                findDataNodesLocation(vertex.getConfig().getListValue("inputdataset"));
            workerPlanMap = calculateDistance(datanodesList, workerPlan, cIdx, allocatedWorkers);
            cal = findOptimalWorkerNode(vertex, workerPlanMap, cIdx);
          } else { //if (cIdx > 0) {
            datanodesList = dataNodeLocatorUtils.
                findDataNodesLocation(vertex.getConfig().getListValue("inputdataset"));
            Worker worker = workerPlan.getWorker(containerIndex);
            //Worker worker = workerPlan.getWorker(cIdx);
            if (dataAwareAllocation.get(containerIndex).size()
                >= maxTaskInstancesPerContainer) {
              try {
                allocatedWorkers.add(worker.getId());
              } catch (NullPointerException ne) {
                ne.printStackTrace();
              }
            }
            workerPlanMap = calculateDistance(datanodesList, workerPlan, cIdx, allocatedWorkers);
            cal = findOptimalWorkerNode(vertex, workerPlanMap, cIdx);
          }
          /*
            This loop allocate the task instances to the respective container, before allocation
            it will check whether the container has reached maximum task instance size which is
            able to hold.
           */
          try {
            for (int i = 0; i < totalNumberOfInstances; i++) {
              containerIndex = Integer.parseInt(Collections.min(cal).getNodeName().trim());
              LOG.info("Worker Node Allocation for task:" + taskName + "(" + i + ")"
                  + "-> Worker:" + containerIndex + "->" + Collections.min(cal).getDataNode());
              if (maxContainerTaskObjectSize < maxTaskInstancesPerContainer) {
                dataAwareAllocation.get(containerIndex).add(
                    new InstanceId(vertex.getName(), globalTaskIndex, i));
                ++maxContainerTaskObjectSize;
              } else {
                LOG.info("Worker:" + containerIndex
                    + "Reached Max. Task Object Size:" + maxContainerTaskObjectSize);
              }
            }
            globalTaskIndex++;
            ++cIdx;
          } catch (NoSuchElementException nse) {
            nse.printStackTrace();
          }
        }
      }
    }

    LOG.info("Container Map Values After Allocation" + dataAwareAllocation);
    for (Map.Entry<Integer, List<InstanceId>> entry : dataAwareAllocation.entrySet()) {
      Integer integer = entry.getKey();
      List<InstanceId> instanceIds = entry.getValue();
      LOG.fine("Container Index:" + integer);
      for (InstanceId instanceId : instanceIds) {
        LOG.fine("Task Details:"
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
  private static Map<String, List<CalculateDataTransferTime>> calculateDistance(
      List<String> datanodesList, WorkerPlan workers,
      int taskIndex, List<Integer> removedWorkers) {

    Map<String, List<CalculateDataTransferTime>> workerPlanMap = new HashMap<>();
    Worker worker;
    double workerBandwidth;
    double workerLatency;
    double calculateDistance = 0.0;
    double datanodeBandwidth;
    double datanodeLatency;

    if (taskIndex == 0) {
      for (String nodesList : datanodesList) {
        ArrayList<CalculateDataTransferTime> calculatedVal = new ArrayList<>();
        for (int i = 0; i < workers.getNumberOfWorkers(); i++) {
          worker = workers.getWorker(i);
          workerBandwidth = (double) worker.getProperty("bandwidth");
          workerLatency = (double) worker.getProperty("latency");

          CalculateDataTransferTime calculateDataTransferTime =
              new CalculateDataTransferTime(nodesList, calculateDistance);

          //For testing just assign the static values
          datanodeBandwidth = 512.0;
          datanodeLatency = 0.4;

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
    } else {
      for (String nodesList : datanodesList) {
        ArrayList<CalculateDataTransferTime> calculatedVal = new ArrayList<>();
        for (int i = 0; i < workers.getNumberOfWorkers(); i++) {
          worker = workers.getWorker(i);

          CalculateDataTransferTime calculateDataTransferTime =
              new CalculateDataTransferTime(nodesList, calculateDistance);

          if (!removedWorkers.contains(worker.getId())) {
            workerBandwidth = (double) worker.getProperty("bandwidth");
            workerLatency = (double) worker.getProperty("latency");

            //For testing just assign the static values
            datanodeBandwidth = 512.0;
            datanodeLatency = 0.4;

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
        }
        workerPlanMap.put(nodesList, calculatedVal);
      }
    }
    return workerPlanMap;
  }

  /**
   * This method finds the worker node which has better network parameters (bandwidth/latency)
   * or it will take lesser time for the data transfer if there is any.
   */
  private static List<CalculateDataTransferTime> findOptimalWorkerNode(Vertex vertex, Map<String,
      List<CalculateDataTransferTime>> workerPlanMap, int i) {

    Set<Map.Entry<String, List<CalculateDataTransferTime>>> entries = workerPlanMap.entrySet();
    List<CalculateDataTransferTime> cal = new ArrayList<>();
    try {
      for (Map.Entry<String, List<CalculateDataTransferTime>> entry : entries) {
        String key = entry.getKey();
        List<CalculateDataTransferTime> value = entry.getValue();
        cal.add(new CalculateDataTransferTime(Collections.min(value).getNodeName(),
            Collections.min(value).getRequiredDataTransferTime(), key));

        for (CalculateDataTransferTime requiredDataTransferTime : value) {
          LOG.fine("Task:" + vertex.getName()
              + "(" + requiredDataTransferTime.getTaskIndex() + ")"
              + key + "D.Node:" + "-> W.Node:" + requiredDataTransferTime.getNodeName()
              + "-> D.Time:" + requiredDataTransferTime.getRequiredDataTransferTime());
        }
      }
    } catch (NoSuchElementException nse) {
      nse.printStackTrace();
    }
    return cal;
  }
}
