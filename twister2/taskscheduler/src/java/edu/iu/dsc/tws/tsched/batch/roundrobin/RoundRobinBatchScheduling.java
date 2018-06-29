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
package edu.iu.dsc.tws.tsched.batch.roundrobin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.task.graph.Vertex;
import edu.iu.dsc.tws.tsched.spi.taskschedule.InstanceId;
import edu.iu.dsc.tws.tsched.utils.TaskAttributes;

public class RoundRobinBatchScheduling {

  private static final Logger LOG = Logger.getLogger(RoundRobinBatchScheduling.class.getName());

  private static int taskId1 = 0;

  protected RoundRobinBatchScheduling() {
  }

  /**
   * This method generate the container -> instance map
   */
  public static Map<Integer, List<InstanceId>> RoundRobinBatchSchedulingAlgo(
      Vertex taskVertex, int numberOfContainers, Config config) {

    TaskAttributes taskAttributes = new TaskAttributes();
    Map<Integer, List<InstanceId>> fifoAllocation = new HashMap<>();
    for (int i = 0; i < numberOfContainers; i++) {
      fifoAllocation.put(i, new ArrayList<>());
    }
    //LOG.info(String.format("Container Map Values Before Allocation %s", fifoAllocation));

    try {
      Map<String, Integer> parallelTaskMap = taskAttributes.getParallelTaskMap(taskVertex);
      int containerIndex = 0;

      for (Map.Entry<String, Integer> e : parallelTaskMap.entrySet()) {
        String task = e.getKey();
        int numberOfInstances = e.getValue();
        for (int taskIndex = 0; taskIndex < numberOfInstances; taskIndex++) {
          fifoAllocation.get(containerIndex).add(new InstanceId(task, taskId1, taskIndex));
          ++containerIndex;
          if (containerIndex >= fifoAllocation.size()) {
            containerIndex = 0;
          }
        }
        taskId1++;
      }
      //LOG.info(String.format("Container Map Values After Allocation %s", fifoAllocation));

      /*for (Map.Entry<Integer, List<InstanceId>> entry : fifoAllocation.entrySet()) {
        Integer integer = entry.getKey();
        List<InstanceId> instanceIds = entry.getValue();
        for (int i = 0; i < instanceIds.size(); i++) {
          LOG.info("Task -> Instance -> Container -> Map Details:"
              + "\t Task Name:" + instanceIds.get(i).getTaskName()
              + "\t Task id:" + instanceIds.get(i).getTaskId()
              + "\t Task index:" + instanceIds.get(i).getTaskIndex()
              + "\t Container Index:" + integer);
        }
      }*/
    } catch (NullPointerException ne) {
      ne.printStackTrace();
    }
    return fifoAllocation;
  }

  /**
   * This method generate the container -> instance map
   */
  public static Map<Integer, List<InstanceId>> RoundRobinBatchSchedulingAlgo(
      Set<Vertex> taskVertexSet, int numberOfContainers, Config config) {

    TaskAttributes taskAttributes = new TaskAttributes();
    Map<Integer, List<InstanceId>> fifoAllocation = new HashMap<>();
    for (int i = 0; i < numberOfContainers; i++) {
      fifoAllocation.put(i, new ArrayList<>());
    }
    try {
      Map<String, Integer> parallelTaskMap = taskAttributes.getParallelTaskMap(taskVertexSet);
      int totalTaskInstances = taskAttributes.getTotalNumberOfInstances(taskVertexSet);

      //if (numberOfContainers < totalTaskInstances) {

      LOG.info(String.format("Container Map Values Before Allocation %s", fifoAllocation));

      int taskId = 0;
      int containerIndex = 0;

      for (Map.Entry<String, Integer> e : parallelTaskMap.entrySet()) {
        String task = e.getKey();
        int numberOfInstances = e.getValue();
        System.out.println("%%% Task Name Is:%%%" + task);

        for (int taskIndex = 0; taskIndex < numberOfInstances; taskIndex++) {
            /*if (fifoAllocation.get(containerIndex).size()
                < TaskSchedulerContext.defaultTaskInstancesPerContainer(config)) {
              System.out.println("I am inside if block");
              fifoAllocation.get(containerIndex).add(new InstanceId(task, taskId, taskIndex));
            } else if (fifoAllocation.get(containerIndex).size()
                > TaskSchedulerContext.defaultTaskInstancesPerContainer(config)) {
              fifoAllocation.get(containerIndex).add(new InstanceId(task, taskId, taskIndex));
              System.out.println("I am inside else block");
            }*/
          fifoAllocation.get(containerIndex).add(new InstanceId(task, taskId, taskIndex));
          ++containerIndex;

          if (containerIndex >= fifoAllocation.size()) {
            containerIndex = 0;
          }
        }
        taskId++;
        //containerIndex++;
      }
      //}
      /*LOG.info(String.format("Container Map Values After Allocation %s", fifoAllocation));
      for (Map.Entry<Integer, List<InstanceId>> entry : fifoAllocation.entrySet()) {
        Integer integer = entry.getKey();
        List<InstanceId> instanceIds = entry.getValue();
        LOG.info("Container Index:" + integer);
        for (int i = 0; i < instanceIds.size(); i++) {
          LOG.info("Task Instance Details:"
              + "\t Task Name:" + instanceIds.get(i).getTaskName()
              + "\t Task id:" + instanceIds.get(i).getTaskId()
              + "\t Task index:" + instanceIds.get(i).getTaskIndex());
        }
      }*/
    } catch (NullPointerException ne) {
      ne.printStackTrace();
    }
    return fifoAllocation;
  }


}
