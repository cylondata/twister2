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
      if ((taskId >= 0 && current.getNodeType() == TreeNode.NodeType.LEAF && current.getTaskId() == taskId &&
          current.stmgrId.equals(stmgrId)) ||
          (taskId < 0 && current.nodeType == NodeType.STMGR && current.stmgrId.equals(stmgrId))) {
        return current;
      } else {
        queue.addAll(current.children);
      }
    }

    return null;
  }

  private TreeNode buildInterGroupTree() {
    // get the stmgrs hosting the component
    List<String> stmgrs = helper.getStmgrsHostingComponent(helper.getMyComponent());
    LOG.log(Level.INFO, "Number of stream managers: " + stmgrs.size());
    if (stmgrs.size() == 0) {
      LOG.log(Level.WARNING, "Stream managers for component is zero: " + helper.getMyComponent());
      return null;
    }

    // sort the list
    Collections.sort(stmgrs);
    TreeNode root = buildIntraNodeTree(stmgrs.get(0));
    if (root == null) {
      LOG.log(Level.WARNING, "Intranode tree didn't built: " + stmgrs.get(0) +
          " : " + helper.getMyComponent());
      return null;
    }
    Queue<TreeNode> queue = new LinkedList<>();
    queue.add(root);

    TreeNode current = queue.poll();
    int i = 1;
    int currentInterNodeDegree = current.children.size() + interNodeDegree;

    while (i < stmgrs.size()) {
      if (current.children.size() < currentInterNodeDegree) {
        TreeNode e = buildIntraGroupTree(stmgrs.get(i));
        if (e != null) {
          current.children.add(e);
          e. = current;
          queue.add(e);
        } else {
          throw new RuntimeException("Stream manager with 0 components for building tree");
        }
        i++;
      } else {
        current = queue.poll();
        currentInterNodeDegree = current.children.size() + interNodeDegree;
      }
    }
    return root;
  }

  private TreeNode buildIntraGroupTree(int groupId) {
    Set<Integer> executorIds = taskPlan.getExecutesOfGroup(groupId);
    if (executorIds.size() == 0) {
      return null;
    }

    List<Integer> executorIdList = new ArrayList<>(executorIds);
    LOG.log(Level.FINE, "Number of tasks: " + executorIds.size());
    // sort the taskIds to make sure everybody creating the same tree
    Collections.sort(executorIdList);

    // get the tasks of this executor
    TreeNode root = createTreeeNode(groupId, executorIdList);

    Queue<TreeNode> queue = new LinkedList<>();
    TreeNode current = root;
    int i = 0;
    while (i < executorIds.size()) {
      int executorId = executorIdList.get(i);
      List<Integer> tasksOfExecutorList =
          new ArrayList<>(taskPlan.getChannelsOfExecutor(executorId));
      Collections.sort(tasksOfExecutorList);

      if (current.getChildren().size() < intraNodeDegree) {
        TreeNode e = new TreeNode(current, executorIds.get(i), groupId,
            helper.getInstanceIdForComponentId(helper.getMyComponent(),
                executorIds.get(i)), NodeType.TASK);
        current.children.add(e);
        e.parent = current;
        queue.add(e);
        i++;
      } else {
        current = queue.poll();
      }
    }

    return root;
  }

  private TreeNode createTreeeNode(int groupId, List<Integer> executorIdList) {
    int rootExecutor = executorIdList.get(0);
    List<Integer> channelsOfExecutorList = new ArrayList<>(
        taskPlan.getChannelsOfExecutor(rootExecutor));
    Collections.sort(channelsOfExecutorList);

    int rootTaskId = channelsOfExecutorList.get(0);
    TreeNode root = new TreeNode(rootTaskId, groupId);
    for (int i = 1; i < channelsOfExecutorList.size(); i++) {
      root.addDirectChild(channelsOfExecutorList.get(i));
    }
    return root;
  }

  private void addDirectChildren() {

  }
}
