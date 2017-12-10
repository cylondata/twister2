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
package edu.iu.dsc.tws.comms.routing;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.comms.core.TaskPlan;

public class LoadBalanceRouter {
  private static final Logger LOG = Logger.getLogger(LoadBalanceRouter.class.getName());
  // the task plan
  private TaskPlan taskPlan;
  // task -> (path -> tasks)
  private Map<Integer, Set<Integer>> externalSendTasks;
  // task -> (path -> tasks)
  private Map<Integer, Set<Integer>> internalSendTasks;
  // task -> (path -> tasks)
  private Map<Integer, List<Integer>> upstream;
  private Set<Integer> receiveExecutors;
  private Set<Integer> thisExecutorTasks;

  /**
   * Create a direct router
   * @param plan
   * @param srscs
   * @param dests
   */
  public LoadBalanceRouter(TaskPlan plan, Set<Integer> srscs, Set<Integer> dests) {
    this.taskPlan = plan;

    this.externalSendTasks = new HashMap<>();
    this.internalSendTasks = new HashMap<>();

    Set<Integer> myTasks = taskPlan.getChannelsOfExecutor(taskPlan.getThisExecutor());
    for (int src : srscs) {
      if (myTasks.contains(src)) {
        for (int dest : dests) {
          // okay the destination is in the same executor
          if (myTasks.contains(dest)) {
            Set<Integer> set = new HashSet<>();
            set.add(dest);
            internalSendTasks.put(src, set);
          } else {
            Set<Integer> set = new HashSet<>();
            set.add(dest);
            externalSendTasks.put(src, set);
          }
        }
      }
    }

    // we are going to receive from all the sources
    this.upstream = new HashMap<>();
    List<Integer> sources = new ArrayList<>();
    sources.addAll(srscs);
    for (int dest : dests) {
      if (myTasks.contains(dest)) {
        this.upstream.put(dest, sources);
      }
    }

    receiveExecutors = LoadBalanceRouter.getExecutorsHostingTasks(plan, srscs);
    // we are not interested in our own
    receiveExecutors.remove(taskPlan.getThisExecutor());

    this.thisExecutorTasks = taskPlan.getChannelsOfExecutor(taskPlan.getThisExecutor());
  }

  public Set<Integer> receivingExecutors() {
    LOG.info(taskPlan.getThisExecutor() + " Receiving executors: " + receiveExecutors);
    return receiveExecutors;
  }

  public Map<Integer, List<Integer>> receiveExpectedTaskIds() {
    // check if this executor contains
    return upstream;
  }

  public boolean isLastReceiver() {
    // now check if destination is in this task
    return true;
  }

  public Map<Integer, Set<Integer>> getInternalSendTasks(int source) {
    // return a routing
    return internalSendTasks;
  }

  public Map<Integer, Set<Integer>> getExternalSendTasks(int source) {
    return externalSendTasks;
  }

  public Map<Integer, Set<Integer>> getExternalSendTasksForPartial(int source) {
    return null;
  }

  public int mainTaskOfExecutor(int executor, int path) {
    return -1;
  }

  /**
   * The destination id is the destination itself
   * @return
   */
  public int destinationIdentifier(int source, int path) {
    return 0;
  }

  public Map<Integer, Integer> getPathAssignedToTasks() {
    return null;
  }

  private static Set<Integer> getExecutorsHostingTasks(TaskPlan plan, Set<Integer> tasks) {
    Set<Integer> executors = new HashSet<>();

    Set<Integer> allExecutors = plan.getAllExecutors();
    LOG.info(String.format("%d All executors: %s", plan.getThisExecutor(), allExecutors));
    for (int e : allExecutors) {
      Set<Integer> tasksOfExecutor = plan.getChannelsOfExecutor(e);
      LOG.info(String.format("%d Tasks of executors: %s", plan.getThisExecutor(), tasksOfExecutor));
      for (int t : tasks) {
        if (tasksOfExecutor.contains(t)) {
          executors.add(e);
          break;
        }
      }
    }

    return executors;
  }
}
