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
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.comms.core.TaskPlan;

public class BinaryTree {
  private static final Logger LOG = Logger.getLogger(BinaryTree.class.getName());

  private int interNodeDegree;
  private int intraNodeDegree;
  private TaskPlan taskPlan;
  private int root;
  private Set<Integer> nodes;
  private int maxLevelsAtExecutor = 0;
  private int maxLevelsAtGroups = 0;

  public BinaryTree(int interNodeDegree, int intraNodeDegree, TaskPlan taskPlan,
                    int source, Set<Integer> destinations) {
    this.interNodeDegree = interNodeDegree;
    this.intraNodeDegree = intraNodeDegree;
    this.taskPlan = taskPlan;
    this.root = source;
    this.nodes = destinations;
    this.maxLevelsAtExecutor = 0;
    LOG.fine(String.format("Building tree with root: %d nodes: %s", root, nodes.toString()));
  }

  public static Node search(Node root, int taskId) {
    Queue<Node> queue = new LinkedList<>();
    queue.add(root);

    while (queue.size() > 0) {
      Node current = queue.poll();
      if (taskId >= 0 && current.getTaskId() == taskId) {
        return current;
      } else {
        queue.addAll(current.getChildren());
      }
    }

    return null;
  }

  /**
   * Retrieve the parent of the child
   *
   * @param root
   * @param taskId
   * @return
   */
  public static Node searchParent(Node root, int taskId) {
    Queue<Node> queue = new LinkedList<>();
    queue.add(root);

    while (queue.size() > 0) {
      Node current = queue.poll();
      if (current.getAllChildrenIds().contains(taskId)) {
        return current;
      } else {
        queue.addAll(current.getChildren());
      }
    }

    return null;
  }

  /**
   * Building the tree
   * @param index
   * @return
   */
  public Node buildInterGroupTree(int index) {
    // first lets get the group hosting the root
    int rootGroup = getGroupHostingTask(root);
    // get the groups hosting the component
    // rotate according to index, this will create a unique tree for each index
    List<Integer> groups = rotateList(new ArrayList<>(getGroupsHostingTasks(nodes)), index);
    LOG.log(Level.FINE, taskPlan.getThisExecutor() + " Groups for binary tree: " + groups);
    if (groups.size() == 0) {
      LOG.log(Level.WARNING, "Groups for destinations is zero");
      return null;
    }

    // lets remove root group from others as we need special treatment here
    groups.remove(new Integer(rootGroup));

    // sort the list
    Collections.sort(groups);
    Node rootNode = buildIntraGroupTree(rootGroup, index);
    if (rootNode == null) {
      LOG.log(Level.WARNING, "Intranode tree didn't built: " + groups.get(0));
      return null;
    }

    Queue<Node> queue = new LinkedList<>();
    Node current = rootNode;

    int i = 0;
    int currentInterNodeDegree = current.getChildren().size() + interNodeDegree;
    while (i < groups.size()) {
      if (current.getChildren().size() < currentInterNodeDegree) {
        Node e = buildIntraGroupTree(groups.get(i), index);
        if (e != null) {
          current.addChild(e);
          e.setParent(current);
          queue.add(e);
          e.setGroupLevel(maxLevelsAtGroups);
        } else {
          throw new RuntimeException("Group with 0 components for building tree");
        }
        i++;
      } else {
        current = queue.poll();
        currentInterNodeDegree = current.getChildren().size() + interNodeDegree;
        maxLevelsAtGroups++;
      }
    }
    return rootNode;
  }

  private Node buildIntraGroupTree(int groupId, int index) {
    // rotate according to index, this will create a unique tree for each index
    Set<Integer> executorsHostingTask = getExecutorsHostingTask(groupId);
//    LOG.log(Level.INFO, taskPlan.getThisExecutor() + " Executor before rotate: "
//        + executorsHostingTask);
    List<Integer> executorIds = rotateList(
        new ArrayList<>(executorsHostingTask), index);

//    LOG.log(Level.INFO, taskPlan.getThisExecutor() + " Executors after rotate: " + executorIds);
    if (executorIds.size() == 0) {
      return null;
    }
    // sort the taskIds to make sure everybody creating the same tree
    Collections.sort(executorIds);
    // now check weather root is part of this group
    int rootExecutor = taskPlan.getExecutorForChannel(root);
    if (executorIds.contains(rootExecutor)) {
      // move the executor to 0
      executorIds.remove(new Integer(rootExecutor));
      executorIds.add(0, rootExecutor);
    }

    int execLevel = 0;
    // create the root of the tree
    Node rootNode = createTreeNode(groupId, executorIds.get(0), index);
    rootNode.setExecLevel(execLevel);
    // now lets create the tree
    Queue<Node> queue = new LinkedList<>();
    Node current = rootNode;
    int i = 1;
    while (i < executorIds.size()) {
      if (current.getChildren().size() < intraNodeDegree) {
        // create a tree node and add it to the current node as a child
        Node e = createTreeNode(groupId, executorIds.get(i), index);
        current.addChild(e);
        e.setParent(current);
        e.setExecLevel(execLevel);
//        LOG.info(String.format("%d Create node with parent %s -> %s",
//            taskPlan.getThisExecutor(), e, current));
        queue.add(e);
        i++;
      } else {
        execLevel++;
        // the current node is filled, lets move to the next
        current = queue.poll();
      }
    }

    if (execLevel > maxLevelsAtExecutor) {
      maxLevelsAtExecutor = execLevel;
    }

    return rootNode;
  }

  private Node createTreeNode(int groupId, int executorId, int rotateIndex) {
    Set<Integer> tasksOfExecutor = getTasksInExecutor(executorId);
    if (tasksOfExecutor == null) {
      throw new RuntimeException("At this point we should have at least one task");
    }

    List<Integer> channelsOfExecutorList = new ArrayList<>(tasksOfExecutor);
    Collections.sort(channelsOfExecutorList);

    // now lets remove root if present and add it to 0th position
    boolean rootPresent = channelsOfExecutorList.contains(root);
    if (rootPresent) {
      channelsOfExecutorList.remove(new Integer(root));
    }

    // we will rotate according to rotate index
    channelsOfExecutorList = rotateList(channelsOfExecutorList, rotateIndex);
    if (rootPresent) {
      channelsOfExecutorList.add(0, root);
    }

    int firstTaskId = channelsOfExecutorList.get(0);
    // this task act as the tree node
    Node node = new Node(firstTaskId, groupId);
    // add all the other tasks as direct children
    for (int i = 1; i < channelsOfExecutorList.size(); i++) {
      node.addDirectChild(channelsOfExecutorList.get(i));
    }
    return node;
  }

  private int getGroupHostingTask(int task) {
    int executor = taskPlan.getExecutorForChannel(task);
    if (executor < 0) {
      String format = String.format("Cannot find executor for task: %d", task);
      LOG.severe(format);
      throw new RuntimeException(format);
    }
    return taskPlan.getGroupOfExecutor(executor);
  }

  private Set<Integer> getTasksInExecutor(int e) {
    Set<Integer> tasks = new HashSet<>();
    Set<Integer> tasksOfExecutor = taskPlan.getChannelsOfExecutor(e);
    if (tasksOfExecutor != null) {
      for (int t: tasksOfExecutor) {
        if (nodes.contains(t) || t == root) {
          tasks.add(t);
        }
      }
    }
    return tasks;
  }

  private Set<Integer> getExecutorsHostingTask(int groupId) {
    Set<Integer> executors = taskPlan.getExecutesOfGroup(groupId);

    Set<Integer> execs = new HashSet<>();
    for (int ex : executors) {
      Set<Integer> tasksOfExec = taskPlan.getChannelsOfExecutor(ex);
      if (tasksOfExec != null) {
        for (int t : nodes) {
          if (tasksOfExec.contains(t)) {
            execs.add(ex);
            break;
          }
        }
        if (tasksOfExec.contains(root)) {
          execs.add(ex);
        }
      }
    }
    return execs;
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

  private List<Integer> rotateList(List<Integer> original, int index) {
    List<Integer> rotate = new ArrayList<>();
    for (int i = 0; i < original.size(); i++) {
      rotate.add(original.get((i + index) % original.size()));
    }
    return rotate;
  }
}
