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
import edu.iu.dsc.tws.tsched.utils.DataTransferTimeCalculator;
import edu.iu.dsc.tws.tsched.utils.TaskAttributes;

/**
 * This class is responsible for scheduling the task graph instances into the worker nodes based on
 * the locality of the data.
 */
public class DataLocalityStreamingScheduler {

  private static final Logger LOG = Logger.getLogger(
      DataLocalityStreamingScheduler.class.getName());

  protected DataLocalityStreamingScheduler() {
  }

  /**
   * This method is primarily responsible for generating the container and task instance map which
   * is based on the task graph, its configuration, and the allocated worker plan.
   */
  public static Map<Integer, List<InstanceId>> dataLocalityAwareSchedulingAlgorithm(
      Set<Vertex> taskVertexSet, int numberOfContainers, WorkerPlan workerPlan, Config config) {

    DataNodeLocatorUtils dataNodeLocatorUtils = new DataNodeLocatorUtils(config);
    TaskAttributes taskAttributes = new TaskAttributes();

    int maxTaskInstancesPerContainer =
        TaskSchedulerContext.defaultTaskInstancesPerContainer(config);
    int containerCapacity = maxTaskInstancesPerContainer * numberOfContainers;
    int totalTask = taskAttributes.getTotalNumberOfInstances(taskVertexSet);

    int index = 0;
    int containerIndex = 0;
    int globalTaskIndex = 0;

    Map<String, Integer> parallelTaskMap = taskAttributes.getParallelTaskMap(taskVertexSet);
    Map<Integer, List<InstanceId>> dataLocalityAwareAllocationMap = new HashMap<>();

    Set<Map.Entry<String, Integer>> taskEntrySet = parallelTaskMap.entrySet();
    List<Integer> allocatedWorkers = new ArrayList<>();

    LOG.fine("No. of Containers:\t" + numberOfContainers
        + "\tMax Task Instances Per Container:\t" + maxTaskInstancesPerContainer);

    //To check the allocated containers can hold all the parallel task instances.

    if (containerCapacity >= totalTask) {
      LOG.fine("Task scheduling can be performed for the container capacity of "
          + containerCapacity + " and " + totalTask + " task instances");
      for (int i = 0; i < numberOfContainers; i++) {
        dataLocalityAwareAllocationMap.put(i, new ArrayList<>());
      }
    } else {
      throw new ScheduleException("Task scheduling can't be performed for the container "
          + "capacity of " + containerCapacity + " and " + totalTask + " task instances");
    }

    LOG.fine("Data Aware Before Task Allocation:\t" + dataLocalityAwareAllocationMap);

    for (Map.Entry<String, Integer> aTaskEntrySet : taskEntrySet) {

      Map<String, List<DataTransferTimeCalculator>> workerPlanMap;
      Map.Entry<String, Integer> entry = aTaskEntrySet;
      String taskName = entry.getKey();

      //If the vertex has input dataset and get the datanode name of the dataset in the HDFS.
      for (Vertex vertex : taskVertexSet) {
        if (vertex.getName().equals(taskName)
            && vertex.getConfig().getListValue("inputdataset") != null) {

          int totalNumberOfInstances = vertex.getParallelism();
          int maxContainerTaskObjectSize = 0;

          List<DataTransferTimeCalculator> cal;
          List<String> datanodesList;

          /*If the index is zero, simply calculate the distance between the worker node and the
            datanodes. Else, if the index values is greater than 0, check the container has reached
            the maximum task instances per container. If it is yes, then allocate the container to
            the allocatedWorkers list which will not be considered for the next scheduling cycle.*/

          if (index == 0) {
            datanodesList = dataNodeLocatorUtils.
                findDataNodesLocation(vertex.getConfig().getListValue("inputdataset"));
            workerPlanMap = distanceCalculation(datanodesList, workerPlan, index, allocatedWorkers);
            cal = findBestWorkerNode(vertex, workerPlanMap);
          } else {
            datanodesList = dataNodeLocatorUtils.
                findDataNodesLocation(vertex.getConfig().getListValue("inputdataset"));
            Worker worker = workerPlan.getWorker(containerIndex);
            //Worker worker = workerPlan.getWorker(index);
            if (dataLocalityAwareAllocationMap.get(containerIndex).size()
                >= maxTaskInstancesPerContainer) {
              try {
                allocatedWorkers.add(worker.getId());
              } catch (NullPointerException ne) {
                ne.printStackTrace();
              }
            }
            workerPlanMap = distanceCalculation(datanodesList, workerPlan, index, allocatedWorkers);
            cal = findBestWorkerNode(vertex, workerPlanMap);
          }

          /*This loop allocate the task instances to the respective container, before allocation
            it will check whether the container has reached maximum task instance size which is
            able to hold.*/

          try {
            for (int i = 0; i < totalNumberOfInstances; i++) {
              containerIndex = Integer.parseInt(Collections.min(cal).getNodeName().trim());
              LOG.fine("Worker Node Allocation for task:" + taskName + "(" + i + ")"
                  + "-> Worker:" + containerIndex + "->" + Collections.min(cal).getDataNode());
              if (maxContainerTaskObjectSize < maxTaskInstancesPerContainer) {
                dataLocalityAwareAllocationMap.get(containerIndex).add(
                    new InstanceId(vertex.getName(), globalTaskIndex, i));
                ++maxContainerTaskObjectSize;
              } else {
                LOG.warning("Worker:" + containerIndex
                    + "Reached Max. Task Object Size:" + maxContainerTaskObjectSize);
              }
            }
            globalTaskIndex++;
            ++index;
          } catch (NoSuchElementException nse) {
            nse.printStackTrace();
          }
        }
      }
    }

    LOG.fine("Container Map Values After Allocation" + dataLocalityAwareAllocationMap);
    for (Map.Entry<Integer, List<InstanceId>> entry : dataLocalityAwareAllocationMap.entrySet()) {
      Integer integer = entry.getKey();
      List<InstanceId> instanceIds = entry.getValue();
      LOG.fine("Container Index:" + integer);
      for (InstanceId instanceId : instanceIds) {
        LOG.fine("Task Details:" + "\tTask Name:" + instanceId.getTaskName()
            + "\tTask id:" + instanceId.getTaskId() + "\tTask index:" + instanceId.getTaskIndex());
      }
    }
    return dataLocalityAwareAllocationMap;
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
    } catch (NoSuchElementException nse) {
      nse.printStackTrace();
    }
    return cal;
  }
}
