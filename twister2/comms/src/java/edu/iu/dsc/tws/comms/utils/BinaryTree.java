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
package edu.iu.dsc.tws.comms.utils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.core.TaskPlan;

public class BinaryTree {
  private static final Logger LOG = Logger.getLogger(BinaryTree.class.getName());

  private Config config;
  private TaskPlan taskPlan;
  private Set<Integer> sources;
  private Set<Integer> destinations;
  private int stream;
  private int task;
  private int intraNodeDegree;
  private int interNodeDegree;

  /**
   * Initialize the data structure
   *
   * @param cfg
   * @param plan
   * @param thisTask
   * @param srscs
   * @param dests
   * @param strm
   */
  public void init(Config cfg, TaskPlan plan, int thisTask,
                   Set<Integer> srscs, Set<Integer> dests, int strm) {
    this.config = cfg;
    this.taskPlan = plan;
    this.task = thisTask;
    this.sources = srscs;
    this.destinations = dests;
    this.stream = strm;
  }

  /**
   * A routing map for each destination
   *
   * Routing is from taskid -> taskids
   *
   * @return a
   */
  public Routing routing() {
    return null;
  }

  private TreeNode search(TreeNode root, int taskId) {
    Queue<TreeNode> queue = new LinkedList<>();
    queue.add(root);

    while (queue.size() > 0) {
      TreeNode current = queue.poll();
      if (taskId >= 0 && current.getTaskId() == taskId) {
        return current;
      } else {
        queue.addAll(current.getChildren());
      }
    }

    return null;
  }

  private TreeNode buildInterGroupTree() {
    // get the groups hosting the component
    List<Integer> groups = new ArrayList<>(getGroupsHostingTasks(destinations));
    LOG.log(Level.INFO, "Number of groups: " + groups.size());
    if (groups.size() == 0) {
      LOG.log(Level.WARNING, "Groups for destinations is zero");
      return null;
    }

    // sort the list
    Collections.sort(groups);
    TreeNode root = buildIntraGroupTree(groups.get(0));
    if (root == null) {
      LOG.log(Level.WARNING, "Intranode tree didn't built: " + groups.get(0));
      return null;
    }

    Queue<TreeNode> queue = new LinkedList<>();
    TreeNode current = root;

    int i = 1;
    int currentInterNodeDegree = current.getChildren().size() + interNodeDegree;
    while (i < groups.size()) {
      if (current.getChildren().size() < currentInterNodeDegree) {
        TreeNode e = buildIntraGroupTree(groups.get(i));
        if (e != null) {
          current.addChild(e);
          e.setParent(current);
          queue.add(e);
        } else {
          throw new RuntimeException("Stream manager with 0 components for building tree");
        }
        i++;
      } else {
        current = queue.poll();
        currentInterNodeDegree = current.getChildren().size() + interNodeDegree;
      }
    }
    return root;
  }

  private TreeNode buildIntraGroupTree(int groupId) {
    List<Integer> executorIds = new ArrayList<>(taskPlan.getExecutesOfGroup(groupId));
    if (executorIds.size() == 0) {
      return null;
    }
    LOG.log(Level.FINE, "Number of executors: " + executorIds.size());

    // sort the taskIds to make sure everybody creating the same tree
    Collections.sort(executorIds);

    // create the root of the tree
    TreeNode root = createTreeeNode(groupId, executorIds.get(0));

    // now lets create the tree
    Queue<TreeNode> queue = new LinkedList<>();
    TreeNode current = root;
    int i = 0;
    while (i < executorIds.size()) {
      if (current.getChildren().size() < intraNodeDegree) {
        // create a tree node and add it to the current node as a child
        TreeNode e = createTreeeNode(groupId, executorIds.get(i));
        current.addChild(e);
        e.setParent(current);
        queue.add(e);
        i++;
      } else {
        // the current node is filled, lets move to the next
        current = queue.poll();
      }
    }

    return root;
  }

  private TreeNode createTreeeNode(int groupId, int executorId) {
    List<Integer> channelsOfExecutorList = new ArrayList<>(
        taskPlan.getChannelsOfExecutor(executorId));
    Collections.sort(channelsOfExecutorList);

    int firstTaskId = channelsOfExecutorList.get(0);
    // this task act as the tree node
    TreeNode node = new TreeNode(firstTaskId, groupId);
    // add all the other tasks as direct children
    for (int i = 1; i < channelsOfExecutorList.size(); i++) {
      node.addDirectChild(channelsOfExecutorList.get(i));
    }
    return node;
  }

  private Set<Integer> getGroupsHostingTasks(Set<Integer> tasks) {
    Set<Integer> groups = new HashSet<>();
    for (int t : tasks) {
      int executor = taskPlan.getExecutorForChannel(t);
      int group = taskPlan.getGroupOfExecutor(executor);
      groups.add(group);
    }
    return groups;
  }
}
