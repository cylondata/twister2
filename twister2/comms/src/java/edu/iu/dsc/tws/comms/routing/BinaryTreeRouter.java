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

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.dfw.DataFlowContext;

public class BinaryTreeRouter {
  private static final Logger LOG = Logger.getLogger(BinaryTreeRouter.class.getName());

  private Map<Integer, List<Integer>> receiveTasks;
  private Set<Integer> receiveExecutors;
  private Map<Integer, Set<Integer>> sendExternalTasksPartial;
  private Map<Integer, Set<Integer>> sendExternalTasks;
  private Map<Integer, Set<Integer>> sendInternalTasks;
  private int mainTask;
  private boolean mainTaskLast;
  private Map<Integer, Integer> destinationIdentifiers;

  /**
   * Initialize the data structure
   *
   * @param cfg
   * @param plan
   * @param root
   * @param dests
   */
  public BinaryTreeRouter(Config cfg, TaskPlan plan,
                                  int root, Set<Integer> dests) {
    int interNodeDegree = DataFlowContext.interNodeDegree(cfg, 2);
    int intraNodeDegree = DataFlowContext.intraNodeDegree(cfg, 2);
    mainTaskLast = false;
    // lets build the tree
    BinaryTree tree = new BinaryTree(interNodeDegree, intraNodeDegree, plan, root, dests);
    Node treeRoot = tree.buildInterGroupTree(0);

    Set<Integer> thisExecutorTasks = plan.getChannelsOfExecutor(plan.getThisExecutor());
    /*
      Tasks belonging to this operation and in the same executor
    */
    Set<Integer> thisExecutorTasksOfOperation = new HashSet<>();
    for (int t : thisExecutorTasks) {
      if (dests.contains(t) || root == t) {
        thisExecutorTasksOfOperation.add(t);
      }
    }
    LOG.fine(String.format("%d Executor Tasks: %s", plan.getThisExecutor(),
        thisExecutorTasksOfOperation.toString()));
    this.destinationIdentifiers = new HashMap<>();
    // construct the map of receiving ids
    this.receiveTasks = new HashMap<>();

    // now lets construct the downstream tasks
    sendExternalTasksPartial = new HashMap<>();
    sendInternalTasks = new HashMap<>();
    sendExternalTasks = new HashMap<>();

    // now lets construct the receive tasks
    receiveExecutors = new HashSet<>();
    for (int t : thisExecutorTasksOfOperation) {
      List<Integer> recv = new ArrayList<>();

      Node search = BinaryTree.search(treeRoot, t);
      // okay this is the main task of this executor
      if (search != null) {
        mainTask = search.getTaskId();
        LOG.fine(String.format("%d main task: %d", plan.getThisExecutor(), mainTask));
        // this is the only task that receives messages and it receive from its parent
        if (search.getParent() != null) {
          receiveExecutors.add(plan.getExecutorForChannel(search.getParent().getTaskId()));
          recv.add(search.getParent().getTaskId());
        }
        if (!recv.isEmpty()) {
          receiveTasks.put(t, new ArrayList<>(recv));
        }

        // this task is connected to others and they dont send messages to anyone
        List<Integer> directChildren = search.getDirectChildren();
        for (int child : directChildren) {
          destinationIdentifiers.put(t, child);

          // we only have one task as main, so we are expecting to receive from it
          List<Integer> childReceiveTasks = new ArrayList<>();
          childReceiveTasks.add(t);
          receiveTasks.put(child, childReceiveTasks);
        }

        // main task is going to send to its internal tasks
        Set<Integer> mainInternalSendTasks = new HashSet<>(directChildren);
        sendInternalTasks.put(t, mainInternalSendTasks);

        // now lets calculate the external send tasks of the main task
        Set<Integer> mainExternalSendTasks = new HashSet<>();
        mainExternalSendTasks.addAll(search.getRemoteChildrenIds());
        sendExternalTasks.put(t, mainExternalSendTasks);
        destinationIdentifiers.put(t, 0);
      } else {
        LOG.fine(String.format("%d doesn't have a node in tree: %d", plan.getThisExecutor(), t));
      }
    }

//    LOG.info(String.format("%d send internal tasks: %s",
//        plan.getThisExecutor(), sendInternalTasks));
//    LOG.info(String.format("%d send external tasks: %s",
//        plan.getThisExecutor(), sendExternalTasks));
//    LOG.info(String.format("%d send externalPartial tasks: %s", plan.getThisExecutor(),
//        sendExternalTasksPartial));
//    LOG.info(String.format("%d receive executor: %s",
//        plan.getThisExecutor(), receiveExecutors));
//    LOG.info(String.format("%d receive tasks: %s",
//        plan.getThisExecutor(), receiveTasks));
  }

  public Set<Integer> receivingExecutors() {
    return receiveExecutors;
  }

  public Map<Integer, List<Integer>> receiveExpectedTaskIds() {
    return receiveTasks;
  }

  public boolean isLastReceiver() {
    // check weather this
    return mainTaskLast;
  }

  public Map<Integer, Set<Integer>> getInternalSendTasks(int source) {
    return sendInternalTasks;
  }

  public Map<Integer, Set<Integer>> getExternalSendTasks(int source) {
    return sendExternalTasks;
  }

  public Map<Integer, Set<Integer>> getExternalSendTasksForPartial(int source) {
    return sendExternalTasksPartial;
  }

  public int mainTaskOfExecutor(int executor, int path) {
    return mainTask;
  }

  public int destinationIdentifier(int source, int path) {
    Object o = destinationIdentifiers.get(source);
    if (o != null) {
      return (int) o;
    } else {
      throw new RuntimeException("Unexpected source requesting destination: " + source);
    }
  }

  public Map<Integer, Integer> getPathAssignedToTasks() {
    return null;
  }

  public Set<Integer> sendQueueIds() {
    Set<Integer> allSends = new HashSet<>();
    allSends.addAll(sendExternalTasks.keySet());
    allSends.addAll(sendInternalTasks.keySet());
    allSends.addAll(sendExternalTasksPartial.keySet());
    return allSends;
  }
}
