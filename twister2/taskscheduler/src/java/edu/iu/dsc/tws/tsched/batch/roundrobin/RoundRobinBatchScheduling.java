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
      Vertex taskVertex, int numberOfContainers) {

    TaskAttributes taskAttributes = new TaskAttributes();
    Map<Integer, List<InstanceId>> roundrobinAllocation = new HashMap<>();
    for (int i = 0; i < numberOfContainers; i++) {
      roundrobinAllocation.put(i, new ArrayList<>());
    }

    //LOG.info(String.format("Container Map Values Before Allocation %s", roundrobinAllocation));

    try {
      Map<String, Integer> parallelTaskMap = taskAttributes.getParallelTaskMap(taskVertex);
      int containerIndex = 0;
      for (Map.Entry<String, Integer> e : parallelTaskMap.entrySet()) {
        String task = e.getKey();
        int numberOfInstances = e.getValue();
        for (int taskIndex = 0; taskIndex < numberOfInstances; taskIndex++) {
          roundrobinAllocation.get(containerIndex).add(new InstanceId(task, taskId1, taskIndex));
          ++containerIndex;
          if (containerIndex >= roundrobinAllocation.size()) {
            containerIndex = 0;
          }
        }
        taskId1++;
      }
      //LOG.info(String.format("Container Map Values After Allocation %s", roundrobinAllocation));
    } catch (NullPointerException ne) {
      ne.printStackTrace();
    }
    return roundrobinAllocation;
  }

  /**
   * This method generate the container -> instance map
   */
  public static Map<Integer, List<InstanceId>> RoundRobinBatchSchedulingAlgo(
      Set<Vertex> taskVertexSet, int numberOfContainers) {

    TaskAttributes taskAttributes = new TaskAttributes();
    Map<Integer, List<InstanceId>> roundrobinAllocation = new HashMap<>();
    for (int i = 0; i < numberOfContainers; i++) {
      roundrobinAllocation.put(i, new ArrayList<>());
    }

    //LOG.info(String.format("Container Map Values Before Allocation %s", roundrobinAllocation));
    try {
      Map<String, Integer> parallelTaskMap = taskAttributes.getParallelTaskMap(taskVertexSet);
      int taskId = 0;
      int containerIndex = 0;

      for (Map.Entry<String, Integer> e : parallelTaskMap.entrySet()) {
        String task = e.getKey();
        int numberOfInstances = e.getValue();
        for (int taskIndex = 0; taskIndex < numberOfInstances; taskIndex++) {
          //roundrobinAllocation.get(containerIndex).add(new InstanceId(task, taskId, taskIndex));
          roundrobinAllocation.get(containerIndex).add(new InstanceId(task, taskId1, taskIndex));
          ++containerIndex;
          if (containerIndex >= roundrobinAllocation.size()) {
            containerIndex = 0;
          }
        }
        taskId1++;
        taskId++;
      }
      //LOG.info(String.format("Container Map Values After Allocation %s", roundrobinAllocation));
    } catch (NullPointerException ne) {
      ne.printStackTrace();
    }
    return roundrobinAllocation;
  }
}
