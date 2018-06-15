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
package edu.iu.dsc.tws.tsched.roundrobin;

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

    TaskAttributes taskAttributes = new TaskAttributes();
    Map<Integer, List<InstanceId>> roundrobinAllocation = new HashMap<>();
    for (int i = 0; i < numberOfContainers; i++) {
      roundrobinAllocation.put(i, new ArrayList<>());
    }
    try {
      Map<String, Integer> parallelTaskMap = taskAttributes.getParallelTaskMap(taskVertexSet);
      int totalTaskInstances = taskAttributes.getTotalNumberOfInstances(taskVertexSet);
      if (numberOfContainers < totalTaskInstances) {
        LOG.info(String.format("Container Map Values Before Allocation %s", roundrobinAllocation));
        int globalTaskIndex = 0;
        for (Map.Entry<String, Integer> e : parallelTaskMap.entrySet()) {
          String task = e.getKey();
          int numberOfInstances = e.getValue();
          int containerIndex;
          for (int i = 0; i < numberOfInstances; i++) {
            containerIndex = i % numberOfContainers;
            roundrobinAllocation.get(containerIndex).add(new InstanceId(task, globalTaskIndex, i));
          }
          globalTaskIndex++;
        }
      }
      LOG.info(String.format("Container Map Values After Allocation %s", roundrobinAllocation));
      for (Map.Entry<Integer, List<InstanceId>> entry : roundrobinAllocation.entrySet()) {
        Integer integer = entry.getKey();
        List<InstanceId> instanceIds = entry.getValue();
        LOG.info("Container Index:" + integer);
        for (int i = 0; i < instanceIds.size(); i++) {
          LOG.info("Task Instance Details:"
              + "\t Task Name:" + instanceIds.get(i).getTaskName()
              + "\t Task id:" + instanceIds.get(i).getTaskId()
              + "\t Task index:" + instanceIds.get(i).getTaskIndex());
        }
      }
    } catch (NullPointerException ne) {
      ne.printStackTrace();
    }
    return roundrobinAllocation;
  }
}

