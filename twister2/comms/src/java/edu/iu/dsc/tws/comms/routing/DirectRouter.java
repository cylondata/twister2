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

import edu.iu.dsc.tws.comms.api.TaskPlan;

public class DirectRouter {
  private static final Logger LOG = Logger.getLogger(DirectRouter.class.getName());
  // destinations
  private List<Integer> destination;
  // task plan
  private TaskPlan taskPlan;
  // task -> (path -> tasks)
  private Map<Integer, Set<Integer>> externalSendTasks;
  // task -> (path -> tasks)
  private Map<Integer, Set<Integer>> internalSendTasks;
  // task -> (path -> tasks)
  private Map<Integer, List<Integer>> upstream;
  // receiving executors
  private Set<Integer> receiveExecutors;
  /// tasks of this executor
  private Set<Integer> thisExecutorTasks;

  /**
   * Create a direct router
   * @param plan
   * @param srscs
   * @param dest
   */
  public DirectRouter(TaskPlan plan, List<Integer> srscs, List<Integer> dest) {
    this.destination = dest;
    this.taskPlan = plan;

    this.externalSendTasks = new HashMap<>();
    this.internalSendTasks = new HashMap<>();

    Set<Integer> myTasks = taskPlan.getChannelsOfExecutor(taskPlan.getThisExecutor());
    for (int i = 0; i < srscs.size(); i++) {
      // for each source we have a fixed target
      int src = srscs.get(i);
      int tar = dest.get(i);
      if (myTasks.contains(src)) {
        // okay the destination is in the same executor
        if (myTasks.contains(tar)) {
          Set<Integer> set = new HashSet<>();
          set.add(tar);
          internalSendTasks.put(src, set);
        } else {
          Set<Integer> set = new HashSet<>();
          set.add(tar);
          externalSendTasks.put(src, set);
        }
      }
    }

    // we are going to receive from one source
    this.upstream = new HashMap<>();
    for (int i = 0; i < srscs.size(); i++) {
      int src = srscs.get(i);
      int tar = dest.get(i);
      List<Integer> sources = new ArrayList<>();
      sources.add(src);
      this.upstream.put(tar, sources);
    }

    // lets calculate the executors we are receiving
    receiveExecutors = new HashSet<>();
    if (isLastReceiver()) {
      for (int s : srscs) {
        int e = taskPlan.getExecutorForChannel(s);
        if (taskPlan.getThisExecutor() != e) {
          receiveExecutors.add(e);
        }
      }
    }
    this.thisExecutorTasks = taskPlan.getChannelsOfExecutor(taskPlan.getThisExecutor());
  }

  public Set<Integer> receivingExecutors() {
    LOG.info(taskPlan.getThisExecutor() + " Receiving executors: " + receiveExecutors);
    return receiveExecutors;
  }

  public Map<Integer, List<Integer>> receiveExpectedTaskIds() {
    return upstream;
  }

  /**
   * Return true if this is a last receiver
   * @return true
   */
  public boolean isLastReceiver() {
    for (int t : destination) {
      // now check if destination is in this worker
      if (taskPlan.getTasksOfThisExecutor().contains(t)) {
        return true;
      }
    }
    return false;
  }

  public Map<Integer, Set<Integer>> getInternalSendTasks(int source) {
    // return a routing
    return internalSendTasks;
  }

  public Map<Integer, Set<Integer>> getExternalSendTasks(int source) {
    return externalSendTasks;
  }
}
