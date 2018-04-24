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
package edu.iu.dsc.tws.tsched.RoundRobin;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.task.graph.Vertex;
import edu.iu.dsc.tws.tsched.spi.taskschedule.InstanceId;
import edu.iu.dsc.tws.tsched.utils.TaskAttributes;

public class RoundRobinScheduling {
  private static final Logger LOG = Logger.getLogger(RoundRobinScheduling.class.getName());

  protected RoundRobinScheduling() {
  }

  /**
   * This method generate the container -> instance map
   */
  public static Map<Integer, List<InstanceId>> RoundRobinSchedulingAlgorithm(
      Set<Vertex> taskVertexSet, int numberOfContainers) {

    /*int globalTaskIndex = 1;
    int taskIndex = 1;
    TaskAttributes taskAttributes = new TaskAttributes();
    Map<Integer, List<InstanceId>> roundrobinAllocation = new HashMap<>();

    try {
      Map<String, Integer> parallelTaskMap = taskAttributes.getParallelTaskMap(taskVertexSet);
      for (int i = 1; i <= numberOfContainers; i++) {
        roundrobinAllocation.put(i, new ArrayList<InstanceId>());
      }
      LOG.info("RR Map Before Allocation\t" + roundrobinAllocation);
      //This value will be replaced with the actual job attributes
      for (String task : parallelTaskMap.keySet()) {
        int numberOfInstances = parallelTaskMap.get(task);
        LOG.info("Task name:" + task + "\t" + "and number of instances:\t"
            + numberOfInstances);
        for (int i = 0; i < numberOfInstances; i++) {
          roundrobinAllocation.get(taskIndex).add(new InstanceId(task, globalTaskIndex, i));
          if (taskIndex != numberOfContainers) {
            taskIndex = taskIndex + 1;
          } else {
            taskIndex = 1;
          }
          LOG.info("Task index and number of containers:\t" + taskIndex
              + "\t" + numberOfContainers);
          globalTaskIndex += 1;
        }
      }
      LOG.info("RR Map After Allocation\t" + roundrobinAllocation);
    } catch (NullPointerException ne) {
      ne.printStackTrace();
    }
    return roundrobinAllocation;*/


    int globalTaskIndex = 0;
    int taskIndex = 0;
    TaskAttributes taskAttributes = new TaskAttributes();
    Map<Integer, List<InstanceId>> roundrobinAllocation = new HashMap<>();
    for (int i = 0; i < numberOfContainers; i++) {
      roundrobinAllocation.put(i, new ArrayList<>());
    }
    try {
      Map<String, Integer> parallelTaskMap = taskAttributes.getParallelTaskMap(taskVertexSet);
      LOG.info("RR Map Before Allocation\t" + roundrobinAllocation);
      for (Map.Entry<String, Integer> e : parallelTaskMap.entrySet()) {
        String task = e.getKey();
        int numberOfInstances = e.getValue();
        LOG.info("Task Name:" + task + "\tNumber Of instances:" + numberOfInstances
            + "\tTaskIndex:" + globalTaskIndex);
        int containerIndex = 0;
        for (int i = 0; i < numberOfInstances; i++) {
          containerIndex = i % numberOfContainers;
          roundrobinAllocation.get(containerIndex).add(new InstanceId(task, globalTaskIndex, i));
        }
        globalTaskIndex++;
      }
      LOG.info(String.format("Container Map Values After Allocation %s", roundrobinAllocation));

    } catch (NullPointerException ne) {
      ne.printStackTrace();
    }
    return roundrobinAllocation;
  }
}

