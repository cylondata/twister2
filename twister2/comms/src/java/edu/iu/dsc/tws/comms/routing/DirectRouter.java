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

import edu.iu.dsc.tws.api.comms.LogicalPlan;

public class DirectRouter {
  private static final Logger LOG = Logger.getLogger(DirectRouter.class.getName());
  // destinations
  private List<Integer> destination;
  // task plan
  private LogicalPlan logicalPlan;
  // task -> (path -> tasks)
  private Map<Integer, Set<Integer>> externalSendTasks;
  // task -> (path -> tasks)
  private Map<Integer, Set<Integer>> internalSendTasks;
  // task -> (path -> tasks)
  private Map<Integer, List<Integer>> upstream;
  // receiving executors
  private Set<Integer> receiveExecutors;

  /**
   * Create a direct router
   */
  public DirectRouter(LogicalPlan plan, List<Integer> srscs, List<Integer> dest) {
    this.destination = dest;
    this.logicalPlan = plan;

    this.externalSendTasks = new HashMap<>();
    this.internalSendTasks = new HashMap<>();

    Set<Integer> myTasks = logicalPlan.getChannelsOfExecutor(logicalPlan.getThisExecutor());
    if (myTasks != null) {
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
    }

    receiveExecutors = new HashSet<>();
    // we are going to receive from one source
    this.upstream = new HashMap<>();
    for (int i = 0; i < srscs.size(); i++) {
      int src = srscs.get(i);
      int tar = dest.get(i);
      if (myTasks != null && myTasks.contains(tar)) {
        List<Integer> sources = new ArrayList<>();
        sources.add(src);
        this.upstream.put(tar, sources);

        // get the executor of source
        int executor = logicalPlan.getExecutorForChannel(src);
        if (executor != logicalPlan.getThisExecutor()) {
          receiveExecutors.add(executor);
        }
      }
    }
  }

  public Set<Integer> receivingExecutors() {
    LOG.fine(logicalPlan.getThisExecutor() + " Receiving executors: " + receiveExecutors);
    return receiveExecutors;
  }

  public Map<Integer, List<Integer>> receiveExpectedTaskIds() {
    return upstream;
  }

  /**
   * Return true if this is a last receiver
   *
   * @return true
   */
  public boolean isLastReceiver() {
    Set<Integer> tasksOfThisExecutor = logicalPlan.getTasksOfThisExecutor();
    for (int t : destination) {
      // now check if destination is in this worker
      if (tasksOfThisExecutor != null && tasksOfThisExecutor.contains(t)) {
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
