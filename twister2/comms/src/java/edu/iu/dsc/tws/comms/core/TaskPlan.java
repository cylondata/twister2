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
package edu.iu.dsc.tws.comms.core;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Information about the instances in which the communication happens.
 * This holds the mapping from the physical level communication addresses which are based on
 * the communication library to high level ids that are defined by upper layers.
 */
public class TaskPlan {
  /**
   * Map from executor to message channel ids, we assume unique channel ids across the cluster
   */
  private Map<Integer, Set<Integer>> executorToChannels = new HashMap<Integer, Set<Integer>>();

  /**
   * channel to executor mapping for easy access
   */
  private Map<Integer, Integer> invertedExecutorToChannels = new HashMap<Integer, Integer>();

  /**
   * Executors can be grouped
   */
  private Map<Integer, Integer> executorToGroup = new HashMap<>();

  /**
   * Group to executor
   */
  private Map<Integer, Set<Integer>> groupsToExecutor = new HashMap<>();

  /**
   * The process under which we are running
   */
  private int thisExecutor;


  public TaskPlan(Map<Integer, Set<Integer>> executorToChannels,
                  Map<Integer, Set<Integer>> groupsToExecutor,
                  int thisExecutor) {
    this.executorToChannels = executorToChannels;
    this.thisExecutor = thisExecutor;
    this.groupsToExecutor = groupsToExecutor;

    for (Map.Entry<Integer, Set<Integer>> e : executorToChannels.entrySet()) {
      for (Integer c : e.getValue()) {
        invertedExecutorToChannels.put(c, e.getKey());
      }
    }

    for (Map.Entry<Integer, Set<Integer>> e : groupsToExecutor.entrySet()) {
      for (Integer ex : e.getValue()) {
        executorToGroup.put(ex, e.getKey());
      }
    }
  }

  public int getExecutorForChannel(int channel) {
    Object ret = invertedExecutorToChannels.get(channel);
    if (ret == null) {
      return -1;
    }
    return (int) ret;
  }

  public Set<Integer> getChannelsOfExecutor(int executor) {
    return executorToChannels.get(executor);
  }

  public int getThisExecutor() {
    return thisExecutor;
  }

  public Set<Integer> getAllExecutors() {
    return executorToChannels.keySet();
  }

  public Set<Integer> getExecutesOfGroup(int group) {
    if (groupsToExecutor.keySet().size() == 0) {
      return new HashSet<>(executorToChannels.keySet());
    }
    return groupsToExecutor.get(group);
  }

  public int getGroupOfExecutor(int executor) {
    if (executorToGroup.containsKey(executor)) {
      return executorToGroup.get(executor);
    }
    return 0;
  }

  public void addChannelToExecutor(int executor, int channel) {
    Set<Integer> values = executorToChannels.get(executor);
    if (values == null) {
      throw new RuntimeException("Cannot add to non-existent worker: " + executor);
    }
    if (values.contains(channel)) {
      throw new RuntimeException("Cannot add existing channel: " + channel);
    }
    values.add(channel);
    invertedExecutorToChannels.put(channel, executor);
  }

  public Set<Integer> getTasksOfThisExecutor() {
    return executorToChannels.get(thisExecutor);
  }

  @Override
  public String toString() {
    return "TaskPlan{"
        + "executorToChannels=" + executorToChannels
        + ", groupsToExecutor=" + groupsToExecutor
        + ", thisExecutor=" + thisExecutor
        + '}';
  }
}
