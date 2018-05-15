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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.task.graph.Vertex;
import edu.iu.dsc.tws.tsched.spi.common.TaskSchedulerContext;
import edu.iu.dsc.tws.tsched.spi.scheduler.Worker;
import edu.iu.dsc.tws.tsched.spi.scheduler.WorkerPlan;
import edu.iu.dsc.tws.tsched.spi.taskschedule.InstanceId;
import edu.iu.dsc.tws.tsched.utils.CalculateDataTransferTime;
import edu.iu.dsc.tws.tsched.utils.DataLocatorUtils;
import edu.iu.dsc.tws.tsched.utils.TaskAttributes;

public class DataLocalityAwareScheduling {

  private static final Logger LOG = Logger.getLogger(DataLocalityAwareScheduling.class.getName());

  //private static int maxContainerTaskObjectSize;

  protected DataLocalityAwareScheduling() {
  }

  /**
   * This method generate the container -> instance map
   */
  public static Map<Integer, List<InstanceId>> DataLocalityAwareSchedulingAlgorithm(
      Set<Vertex> taskVertexSet, int numberOfContainers, WorkerPlan workerPlan, Config config) {

    int maxTaskInstancesPerContainer =
        TaskSchedulerContext.defaultTaskInstancesPerContainer(config);
    int maxContainerTaskObjectSize;

    DataLocatorUtils dataLocatorUtils;
    TaskAttributes taskAttributes = new TaskAttributes();

    Map<String, Integer> parallelTaskMap = taskAttributes.getParallelTaskMap(taskVertexSet);
    Map<Integer, List<InstanceId>> dataAwareAllocation = new HashMap<>();
    Set<Map.Entry<String, Integer>> entries = parallelTaskMap.entrySet();
    List<Integer> allocatedWorkers = new ArrayList<>();

    int containerCapacity = maxTaskInstancesPerContainer * numberOfContainers;
    int totalTask = taskAttributes.getTotalNumberOfInstances(taskVertexSet);
    int cIdx = 0;
    int containerIndex = 0;

    if (containerCapacity >= totalTask) {
      LOG.info("Task Scheduling Can be Performed for the Container Capacity of "
          + containerCapacity + " and " + totalTask + " Task Instances");

      for (int i = 0; i < numberOfContainers; i++) {
        dataAwareAllocation.put(i, new ArrayList<>());
      }

      LOG.info(String.format("Data Aware Before Task Allocation:" + dataAwareAllocation + "\n"
          + "Parallel Task Map Details:" + parallelTaskMap.entrySet()));

      for (Iterator<Map.Entry<String, Integer>> iterator = entries.iterator();
           iterator.hasNext();) {

        Map<String, List<CalculateDataTransferTime>> workerPlanMap;
        Map.Entry<String, Integer> entry = iterator.next();
        String key = entry.getKey();

        for (Vertex vertex : taskVertexSet) {
          if (vertex.getName().equals(key)
              && vertex.getConfig().getListValue("dataset") != null) {
            List<String> datasetList = vertex.getConfig().getListValue("dataset");
            if (datasetList.size() == 1) {
              String datasetName = datasetList.get(0);
              int totalNumberOfInstances = vertex.getParallelism();
              int globalTaskIndex = 0;
              dataLocatorUtils = new DataLocatorUtils(datasetName);
              List<String> datanodesList = dataLocatorUtils.findDataNodes();
              List<CalculateDataTransferTime> cal = null;
              if (cIdx == 0) {
                workerPlanMap = calculateDistance(
                    datanodesList, workerPlan, cIdx, allocatedWorkers);
                cal = findOptimalWorkerNode(vertex, workerPlanMap, cIdx);
              } else if (cIdx > 0) {
                Worker worker = workerPlan.getWorker(containerIndex);
                if (dataAwareAllocation.get(containerIndex).size()
                    >= maxTaskInstancesPerContainer) {
                  try {
                    allocatedWorkers.add(worker.getId());
                  } catch (NullPointerException ne) {
                    ne.printStackTrace();
                  }
                }
                workerPlanMap = calculateDistance(
                    datanodesList, workerPlan, cIdx, allocatedWorkers);
                cal = findOptimalWorkerNode(vertex, workerPlanMap, cIdx);
              }

            /*System.out.println(String.format(cal.toString()) + "\tAnd Its Size:" + cal.size());
            System.out.println(String.format("Optimal Worker Node Details:"
                + cal.size() + "\tworker:" + Collections.min(cal).getNodeName()) + "\t"
                + Collections.min(cal).getRequiredDataTransferTime());*/

              maxContainerTaskObjectSize = 0;
              for (int i = 0; i < totalNumberOfInstances; i++) {
                containerIndex = Integer.parseInt(Collections.min(cal).getNodeName().trim());
                LOG.info("Worker Node Allocation for task:" + vertex.getName() + "(" + i + ")"
                    + "-> Worker:" + containerIndex + "->" + Collections.min(cal).getDataNode());
                if (maxContainerTaskObjectSize < maxTaskInstancesPerContainer) {
                  dataAwareAllocation.get(containerIndex).add(
                      new InstanceId(vertex.getName(), globalTaskIndex, i));
                  globalTaskIndex++;
                  maxContainerTaskObjectSize++;
                } else {
                  LOG.info(String.format("Worker:" + containerIndex
                      + "-> Reached Max. Task Object Size:" + maxContainerTaskObjectSize));
                }
              }
              ++cIdx;
              LOG.info(String.format("********************************************************"));
            }
          }
        }
      }
    } else {
      LOG.info("Task Scheduling Can't be Performed for the Container Capacity of "
          + containerCapacity + " and " + totalTask + " Task Instances");
    }
    LOG.info(String.format("Data Aware After Task Allocation:" + dataAwareAllocation));
    return dataAwareAllocation;
  }

  /**
   * It calculates the distance between the data nodes and the worker nodes.
   */
  public static Map<String, List<CalculateDataTransferTime>> calculateDistance(
      List<String> datanodesList, WorkerPlan workers, int taskIndex, List<Integer> removedWorkers) {

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
            //Just for testing assigned static values and static increment...!
            if ("datanode1".equals(nodesList)) {
              datanodeBandwidth = 1024.0;
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
        }
        workerPlanMap.put(nodesList, calculatedVal);
      }
    }
    return workerPlanMap;
  }

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
        for (CalculateDataTransferTime requiredDataTransferTime : value) {
          LOG.info(String.format("Task:" + vertex.getName() + "("
              + requiredDataTransferTime.getTaskIndex() + ")"
              + "D.Node:" + key + "-> W.Node:" + requiredDataTransferTime.getNodeName()
              + "-> D.Time:" + requiredDataTransferTime.getRequiredDataTransferTime()));
        }

        /*cal.add(new CalculateDataTransferTime(Collections.min(value).getNodeName(),
          Collections.min(value).getRequiredDataTransferTime()));*/

        cal.add(new CalculateDataTransferTime(Collections.min(value).getNodeName(),
            Collections.min(value).getRequiredDataTransferTime(), key));
      }
    } catch (NoSuchElementException nse) {
      nse.printStackTrace();
    }
    LOG.info(String.format("********************************************************"));
    return cal;
  }
}
