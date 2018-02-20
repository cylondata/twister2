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
package edu.iu.dsc.tws.examples.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.rsched.core.SchedulerContext;
import edu.iu.dsc.tws.rsched.spi.resource.ResourceContainer;
import edu.iu.dsc.tws.rsched.spi.resource.ResourcePlan;

public final class WordCountUtils {
  private static final Logger LOG = Logger.getLogger(WordCountUtils.class.getName());

  private WordCountUtils() {
  }

  /**
   * Let assume we have 2 tasks per container and one additional for first container,
   * which will be the destination
   * @param plan the resource plan from scheduler
   * @return task plan
   */
  public static TaskPlan createWordCountPlan(Config cfg, ResourcePlan plan, int noOfTasks) {
    int noOfProcs = plan.noOfContainers();
    LOG.log(Level.INFO, "No of containers: " + noOfProcs);
    Map<Integer, Set<Integer>> executorToGraphNodes = new HashMap<>();
    Map<Integer, Set<Integer>> groupsToExeuctors = new HashMap<>();
    int thisExecutor = plan.getThisId();

    List<ResourceContainer> containers = plan.getContainers();
    Map<String, List<ResourceContainer>> containersPerNode = new HashMap<>();
    for (ResourceContainer c : containers) {
      String name = (String) c.getProperty(SchedulerContext.WORKER_NAME);
      List<ResourceContainer> containerList;
      if (!containersPerNode.containsKey(name)) {
        containerList = new ArrayList<>();
        containersPerNode.put(name, containerList);
      } else {
        containerList = containersPerNode.get(name);
      }
      containerList.add(c);
    }

    int taskPerExecutor = noOfTasks / noOfProcs;
    for (int i = 0; i < noOfProcs; i++) {
      Set<Integer> nodesOfExecutor = new HashSet<>();
      for (int j = 0; j < taskPerExecutor; j++) {
        nodesOfExecutor.add(i * taskPerExecutor + j);
      }
      executorToGraphNodes.put(i, nodesOfExecutor);
    }

    int i = 0;
    // we take each container as an executor
    for (Map.Entry<String, List<ResourceContainer>> e : containersPerNode.entrySet()) {
      Set<Integer> executorsOfGroup = new HashSet<>();
      for (ResourceContainer c : e.getValue()) {
        executorsOfGroup.add(c.getId());
      }
      groupsToExeuctors.put(i, executorsOfGroup);
      i++;
    }

    LOG.fine(String.format("%d Executor To Graph: %s", plan.getThisId(), executorToGraphNodes));
    LOG.fine(String.format("%d Groups to executors: %s", plan.getThisId(), groupsToExeuctors));
    // now lets create the task plan of this, we assume we have map tasks in all the processes
    // and reduce task in 0th process
    return new TaskPlan(executorToGraphNodes, groupsToExeuctors, thisExecutor);
  }
}
