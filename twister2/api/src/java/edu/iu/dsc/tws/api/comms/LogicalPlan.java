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
package edu.iu.dsc.tws.api.comms;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Information about the instances in which the communication happens.
 * This holds the mapping from the physical level communication addresses which are based on
 * the communication library to high level ids that are defined by upper layers.
 */
public class LogicalPlan {
  /**
   * Map from executor to message channel ids, we assume unique channel ids across the cluster
   */
  private Map<Integer, Set<Integer>> workerToLogicalId = new HashMap<Integer, Set<Integer>>();

  /**
   * channel to executor mapping for easy access
   */
  private Map<Integer, Integer> invertedWorkerToLogicalIds = new HashMap<Integer, Integer>();

  /**
   * Executors can be grouped
   */
  private Map<Integer, Integer> workerToGroup = new HashMap<>();

  /**
   * Group to executor
   */
  private Map<Integer, Set<Integer>> workerGroups = new HashMap<>();

  /**
   * Map from worker to tasks
   */
  private Map<String, Set<Integer>> nodeToLogicalId;

  /**
   * The process under which we are running
   */
  private int thisWorker;


  public LogicalPlan(Map<Integer, Set<Integer>> workerToLogicalId,
                     Map<Integer, Set<Integer>> workerGroups,
                     Map<String, Set<Integer>> nodeToLogicalId,
                     int thisWorker) {
    this.workerToLogicalId = workerToLogicalId;
    this.nodeToLogicalId = nodeToLogicalId;
    this.thisWorker = thisWorker;
    this.workerGroups = workerGroups;

    for (Map.Entry<Integer, Set<Integer>> e : workerToLogicalId.entrySet()) {
      for (Integer c : e.getValue()) {
        invertedWorkerToLogicalIds.put(c, e.getKey());
      }
    }

    for (Map.Entry<Integer, Set<Integer>> e : workerGroups.entrySet()) {
      for (Integer ex : e.getValue()) {
        workerToGroup.put(ex, e.getKey());
      }
    }
  }

  public int getWorkerForForLogicalId(int channel) {
    Object ret = invertedWorkerToLogicalIds.get(channel);
    if (ret == null) {
      return -1;
    }
    return (int) ret;
  }

  public Set<Integer> getLogicalIdsOfWorker(int worker) {
    return workerToLogicalId.getOrDefault(worker, Collections.emptySet());
  }

  public int getThisWorker() {
    return thisWorker;
  }

  public Set<Integer> getAllWorkers() {
    return workerToLogicalId.keySet();
  }

  public Set<Integer> getWorkersOfGroup(int group) {
    if (workerGroups.keySet().size() == 0) {
      return new HashSet<>(workerToLogicalId.keySet());
    }
    return workerGroups.get(group);
  }

  public int getGroupOfWorker(int worker) {
    if (workerToGroup.containsKey(worker)) {
      return workerToGroup.get(worker);
    }
    return 0;
  }

  public void addLogicalIdToWorker(int worker, int logicalId) {
    Set<Integer> values = workerToLogicalId.get(worker);
    if (values == null) {
      throw new RuntimeException("Cannot add to non-existent worker: " + worker);
    }
    if (values.contains(logicalId)) {
      throw new RuntimeException("Cannot add existing channel: " + logicalId);
    }
    values.add(logicalId);
    invertedWorkerToLogicalIds.put(logicalId, worker);
  }

  public Set<Integer> getLogicalIdsOfThisWorker() {
    return workerToLogicalId.get(thisWorker);
  }

  public Map<String, Set<Integer>> getNodeToLogicalId() {
    return nodeToLogicalId;
  }

  public int getIndexOfTaskInNode(int task) {
    Optional<Set<Integer>> tasksInThisNode = this.getNodeToLogicalId().values()
        .stream().filter(tasks -> tasks.contains(task)).findFirst();
    if (tasksInThisNode.isPresent()) {
      List<Integer> sortedTargets = tasksInThisNode.get().stream().sorted()
          .collect(Collectors.toList());
      return sortedTargets.indexOf(task);
    } else {
      throw new RuntimeException("Couldn't find task in any node");
    }
  }

  @Override
  public String toString() {
    return "LogicalPlan{"
        + "workerToLogicalId=" + workerToLogicalId
        + ", workerGroups=" + workerGroups
        + ", thisWorker=" + thisWorker
        + '}';
  }
}
