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
package edu.iu.dsc.tws.tsched.batch.datalocality;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
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
import edu.iu.dsc.tws.tsched.utils.CalculateDataTransferTime;
import edu.iu.dsc.tws.tsched.utils.TaskAttributes;

/**
 * This class is responsible for scheduling the task graph instances into the worker nodes
 * based on the locality of the data.
 */
public class DataLocalityBatchScheduling {

  private static final Logger LOG = Logger.getLogger(DataLocalityBatchScheduling.class.getName());
  private static int globalTaskIndex = 0;

  protected DataLocalityBatchScheduling() {
  }

  /**
   * This method is primarily responsible for generating the container and task instance map which
   * is based on the task graph, its configuration, and the allocated worker plan.
   */
  public static Map<Integer, List<InstanceId>> DataLocalityBatchSchedulingAlgo(
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

    for (Iterator<Map.Entry<String, Integer>> iterator = taskEntrySet.iterator();
         iterator.hasNext();) {

      Map<String, List<CalculateDataTransferTime>> workerPlanMap;
      Map.Entry<String, Integer> entry = iterator.next();
      String taskName = entry.getKey();

      /**
       * If the vertex has the input data set list and get the status
       * and the path of the file in HDFS.
       */

      if (taskVertex.getName().equals(taskName)
          && (taskVertex.getConfig().getListValue("inputdataset") != null)) {

        int totalNumberOfInstances = taskVertex.getParallelism();
        List<CalculateDataTransferTime> cal = null;
        List<String> datanodesList;

        /**
         * If the cIdx is zero, simply calculate the distance between the worker node and
         * the datanodes. Else, if the cIdx values is greater than 0, check the container
         * has reached the maximum task instances per container. If it is yes, then allocate
         * the container to the allocatedWorkers list which will not be considered for the
         * next scheduling cycle.
         */

        if (cIdx == 0) {
          datanodesList = dataNodeLocatorUtils.
              findDataNodesLocation(taskVertex.getConfig().getListValue("inputdataset"));
          workerPlanMap = calculateDistanceforBatch(datanodesList, workerPlan, cIdx);
          cal = findOptimalWorkerNode(taskVertex, workerPlanMap, cIdx);
        }

        /**
         * This loop allocate the task instances to the respective container, before allocation
         * it will check whether the container has reached maximum task instance size which is
         * able to hold.
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
      for (int i = 0; i < instanceIds.size(); i++) {
        LOG.fine("Task Instance Scheduled Details:"
            + "-> Task Name:" + instanceIds.get(i).getTaskName()
            + "-> Task id:" + instanceIds.get(i).getTaskId()
            + "-> Task index:" + instanceIds.get(i).getTaskIndex());
      }
    }
    return dataAwareAllocation;
  }

  /**
   * This method is primarily responsible for generating the container and task instance map which
   * is based on the task graph, its configuration, and the allocated worker plan.
   */
  public static Map<Integer, List<InstanceId>> DataLocalityBatchSchedulingAlgo(
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

    for (Iterator<Map.Entry<String, Integer>> iterator = taskEntrySet.iterator();
         iterator.hasNext();) {

      Map<String, List<CalculateDataTransferTime>> workerPlanMap;
      Map.Entry<String, Integer> entry = iterator.next();
      String taskName = entry.getKey();

      /**
       * If the vertex has the input data set list and get the status
       * and the path of the file in HDFS.
       */
      for (Vertex vertex : taskVertexSet) {
        if (vertex.getName().equals(taskName)
            && vertex.getConfig().getListValue("inputdataset") != null) {

          int totalNumberOfInstances = vertex.getParallelism();
          List<CalculateDataTransferTime> cal = null;
          List<String> datanodesList;

          if (cIdx == 0) {
            datanodesList = dataNodeLocatorUtils.
                findDataNodesLocation(vertex.getConfig().getListValue("inputdataset"));
            workerPlanMap = calculateDistanceforBatch(datanodesList, workerPlan, cIdx);
            cal = findOptimalWorkerNode(vertex, workerPlanMap, cIdx);
          }

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
      LOG.fine("Container Index:" + integer);
      for (int i = 0; i < instanceIds.size(); i++) {
        LOG.fine("Task Details:"
            + "\t Task Name:" + instanceIds.get(i).getTaskName()
            + "\t Task id:" + instanceIds.get(i).getTaskId()
            + "\t Task index:" + instanceIds.get(i).getTaskIndex());
      }
    }
    return dataAwareAllocation;
  }

  /**
   * It calculates the distance between the data nodes and the worker nodes.
   */
  public static Map<String, List<CalculateDataTransferTime>> calculateDistanceforBatch(
      List<String> datanodesList, WorkerPlan workers, int taskIndex) {

    Map<String, List<CalculateDataTransferTime>> workerPlanMap = new HashMap<>();
    Worker worker;
    double workerBandwidth = 0.0;
    double workerLatency = 0.0;
    double calculateDistance = 0.0;
    double datanodeBandwidth;
    double datanodeLatency;

    for (String nodesList : datanodesList) {
      ArrayList<CalculateDataTransferTime> calculatedVal = new ArrayList<>();
      for (int i = 0; i < workers.getNumberOfWorkers(); i++) {
        worker = workers.getWorker(i);

        try {
          workerBandwidth = (double) worker.getProperty("bandwidth");
          workerLatency = (double) worker.getProperty("latency");
        } catch (NullPointerException ne) {
          ne.printStackTrace();
        }

        CalculateDataTransferTime calculateDataTransferTime =
            new CalculateDataTransferTime(nodesList, calculateDistance);

        //Just for testing assigned static values and static increment...!
        if ("datanode1".equals(nodesList)) {
          datanodeBandwidth = 512.0;
          datanodeLatency = 0.4;
        } else {
          datanodeBandwidth = 512.0; //assign some other bandwidth value
          datanodeLatency = 0.4;
        }

        //Write the proper formula to calculate the distance between
        //worker nodes and data nodes.
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
  public static List<CalculateDataTransferTime> findOptimalWorkerNode(Vertex vertex, Map<String,
      List<CalculateDataTransferTime>> workerPlanMap, int i) {

    Set<Map.Entry<String, List<CalculateDataTransferTime>>> entries = workerPlanMap.entrySet();
    List<CalculateDataTransferTime> cal = new ArrayList<>();

    try {
      for (Iterator<Map.Entry<String, List<CalculateDataTransferTime>>> iterator
           = entries.iterator(); iterator.hasNext();) {

        Map.Entry<String, List<CalculateDataTransferTime>> entry = iterator.next();
        String key = entry.getKey();
        List<CalculateDataTransferTime> value = entry.getValue();

        /*cal.add(new CalculateDataTransferTime(Collections.min(value).getNodeName(),
            Collections.min(value).getRequiredDataTransferTime(), key));*/

        for (int j = 0; j < value.size(); j++) {
          cal.add(new CalculateDataTransferTime(value.get(j).getNodeName(),
              value.get(j).getRequiredDataTransferTime(), key));
        }
      }
    } catch (NoSuchElementException nse) {
      nse.printStackTrace();
    }
    return cal;
  }
}

