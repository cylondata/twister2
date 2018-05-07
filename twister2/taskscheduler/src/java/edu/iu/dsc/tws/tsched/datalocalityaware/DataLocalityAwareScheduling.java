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
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.task.graph.Vertex;
import edu.iu.dsc.tws.tsched.spi.scheduler.Worker;
import edu.iu.dsc.tws.tsched.spi.scheduler.WorkerPlan;
import edu.iu.dsc.tws.tsched.spi.taskschedule.InstanceId;
import edu.iu.dsc.tws.tsched.utils.CalculateDataTransferTime;
import edu.iu.dsc.tws.tsched.utils.DataLocatorUtils;
import edu.iu.dsc.tws.tsched.utils.TaskAttributes;

public class DataLocalityAwareScheduling {

  private static final Logger LOG = Logger.getLogger(DataLocalityAwareScheduling.class.getName());

  protected DataLocalityAwareScheduling() {
  }

  /**
   * This method generate the container -> instance map
   */
  public static Map<Integer, List<InstanceId>> DataLocalityAwareSchedulingAlgorithm(
      Set<Vertex> taskVertexSet, int numberOfContainers, WorkerPlan workerPlan) {

    TaskAttributes taskAttributes = new TaskAttributes();
    DataLocatorUtils dataLocatorUtils;
    Map<String, Integer> parallelTaskMap = taskAttributes.getParallelTaskMap(taskVertexSet);

    LOG.info("Parallel Task Map Details:" + parallelTaskMap.entrySet());

    Set<Map.Entry<String, Integer>> entries = parallelTaskMap.entrySet();

    for (Iterator<Map.Entry<String, Integer>> iterator
         = entries.iterator(); iterator.hasNext();) {
      Map.Entry<String, Integer> entry = iterator.next();
      String key = entry.getKey();
      Integer value = entry.getValue();
      Map<String, List<CalculateDataTransferTime>> workerPlanMap = null;
      for (Vertex vertex : taskVertexSet) {
        if (vertex.getName().equals(key)
            && vertex.getConfig().getListValue("dataset") != null) {
          List<String> datasetList = vertex.getConfig().getListValue("dataset");
          LOG.info("Key Value exists in the taskset:" + key + "\t" + value
              + "\t" + datasetList.size());
          if (datasetList.size() == 1) {
            String datasetName = datasetList.get(0);
            dataLocatorUtils = new DataLocatorUtils(datasetName);
            List<String> datanodesList = dataLocatorUtils.findDataNodes();
            int totalNumberOfInstances = vertex.getParallelism();
            for (int i = 0; i < totalNumberOfInstances; i++) {
              workerPlanMap = calculateDistance(datanodesList, workerPlan, i);
              print(vertex, workerPlanMap, i);
            }
          } /*else if (datasetList.size() > 1) {
            for (int i = 0; i < datasetList.size(); i++) {
              String datasetName = datasetList.get(i);
              dataLocatorUtils = new DataLocatorUtils(datasetName);
              datanodesList = dataLocatorUtils.findDataNodes();
              int totalNumberOfInstances = vertex.getParallelism();
              for (int j = 0; j < totalNumberOfInstances; j++) {
                workerPlanMap = calculateDistance(datanodesList, workerPlan, i);
                print(vertex, workerPlanMap);
              }
            }
          }*/
        }
      }
    }

    Map<Integer, List<InstanceId>> dataAwareAllocation = new HashMap<>();
    for (int i = 0; i < numberOfContainers; i++) {
      dataAwareAllocation.put(i, new ArrayList<>());
    }
    LOG.info("DataAware Allocation:" + dataAwareAllocation);
    return dataAwareAllocation;
  }

  public static void print(Vertex vertex, Map<String,
      List<CalculateDataTransferTime>> workerPlanMap, int i) {

    Set<Map.Entry<String, List<CalculateDataTransferTime>>> entries = workerPlanMap.entrySet();

    List<CalculateDataTransferTime> cal = new ArrayList<>();

    for (Iterator<Map.Entry<String, List<CalculateDataTransferTime>>> iterator
         = entries.iterator(); iterator.hasNext();) {
      Map.Entry<String, List<CalculateDataTransferTime>> entry = iterator.next();
      String key = entry.getKey();
      List<CalculateDataTransferTime> value = entry.getValue();
      /*for (CalculateDataTransferTime requiredDataTransferTime : value) {
        System.out.println(String.format("Task Vertex:" + vertex.getName() + "("
            + requiredDataTransferTime.getTaskIndex() + ")"
            + "Data Node:" + key + "--> WorkerNode:" + requiredDataTransferTime.getNodeName()
            + "-->Time:" + requiredDataTransferTime.getRequiredDataTransferTime()));
      }*/
      System.out.println(String.format("\nFor Task Vertex:" + vertex.getName() + "(" + i + ")"
          + "\tData Node:" + key
          + "\tWorker Node:" + Collections.min(value).getNodeName()
          + "\tMin Data Trans. Time:" + Collections.min(value).getRequiredDataTransferTime()));

      cal.add(new CalculateDataTransferTime(Collections.min(value).getNodeName(),
          Collections.min(value).getRequiredDataTransferTime()));
    }
    //cal.forEach(System.out::println);
    System.out.println(String.format("Cal Size:" + cal.size() + "\t"
        + Collections.min(cal).getRequiredDataTransferTime()) + "\t"
        + Collections.min(cal).getNodeName());
  }

  /**
   * It calculates the distance between the data nodes and the worker nodes.
   */
  public static Map<String, List<CalculateDataTransferTime>> calculateDistance(
      List<String> datanodesList, WorkerPlan workers, int taskIndex) {

    Map<String, List<CalculateDataTransferTime>> workerPlanMap
        = new HashMap<String, List<CalculateDataTransferTime>>();
    for (String nodesList : datanodesList) {
      ArrayList<CalculateDataTransferTime> calculatedVal = new ArrayList<>();
      for (int i = 0; i < workers.getNumberOfWorkers(); i++) {
        Worker worker = workers.getWorker(i);
        double workerBandwidth = (double) worker.getProperty("bandwidth");
        double workerLatency = (double) worker.getProperty("latency");
        double calculateDistance = 0.0;
        double datanodeBandwidth;
        double datanodeLatency;

        CalculateDataTransferTime calculateDataTransferTime =
            new CalculateDataTransferTime(nodesList, calculateDistance);

        //Just for testing assigned static values and static increment...!
        if ("datanode1".equals(nodesList)) {
          datanodeBandwidth = 512.0;
          datanodeLatency = 0.4;
        } else {
          datanodeBandwidth = 512.0 + 100.0;
          datanodeLatency = 0.4 + 0.2;
        }
        //Write the proper formula to calculate the distance between
        //worker nodes and data nodes.
        calculateDistance = Math.abs((2 * workerBandwidth * workerLatency)
            - (2 * datanodeBandwidth * datanodeLatency));

        //(use this formula to calculate the data transfer time)
        //calculateDistance = File Size / Bandwidth;

        calculateDataTransferTime.setRequiredDataTransferTime(calculateDistance);
        calculateDataTransferTime.setNodeName("worker" + i);
        calculateDataTransferTime.setTaskIndex(taskIndex);
        calculatedVal.add(calculateDataTransferTime);
      }
      //workerPlanMap.put("worker" + i, calculatedValues);
      //workerPlanMap.put("worker" + i, calculatedVal);
      workerPlanMap.put(nodesList, calculatedVal);
    }
    return workerPlanMap;
  }

  public static double calculateDistance(List<String> datanodesList, List<Worker> workers) {
    for (String nodesList : datanodesList) {
      for (Worker worker : workers) {
        double workerBandwidth = (double) worker.getProperty("bandwidth");
        double workerLatency = (double) worker.getProperty("latency");
        double datanodeBandwidth = 512.0;
        double datanodeLatency = 0.01;
        double calculateDistance = Math.abs(2 * workerBandwidth * workerLatency)
            - (2 * datanodeBandwidth * datanodeLatency);
        LOG.info("Data Node Name:" + nodesList + "\t:" + worker.getId() + ":" + calculateDistance);
      }
    }
    return 1.0;
  }

  private ArrayList<CalculateDataTransferTime> getSortedDataTransferTime(Set<String> taskNameSet) {
    ArrayList<CalculateDataTransferTime> ramRequirements = new ArrayList<>();
    Collections.sort(ramRequirements, Collections.reverseOrder());
    return ramRequirements;
  }
}


