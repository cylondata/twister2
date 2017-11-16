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
import edu.iu.dsc.tws.comms.mpi.MPIContext;

public class BinaryTreeRouter implements IRouter {
  private static final Logger LOG = Logger.getLogger(BinaryTreeRouter.class.getName());

  private Map<Integer, Map<Integer, List<Integer>>> receiveTasks;
  private Set<Integer> receiveExecutors;
  private Map<Integer, Map<Integer, Set<Integer>>> sendExternalTasksPartial;
  private Map<Integer, Map<Integer, Set<Integer>>> sendExternalTasks;
  private Map<Integer, Map<Integer, Set<Integer>>> sendInternalTasks;
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
    int interNodeDegree = MPIContext.interNodeDegree(cfg, 2);
    int intraNodeDegree = MPIContext.intraNodeDegree(cfg, 2);
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
    LOG.info(String.format("%d Executor Tasks: %s", plan.getThisExecutor(),
        thisExecutorTasksOfOperation.toString()));
    this.destinationIdentifiers = new HashMap<>();
    // construct the map of receiving ids
    this.receiveTasks = new HashMap<Integer, Map<Integer, List<Integer>>>();

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
        LOG.info(String.format("%d main task: %d", plan.getThisExecutor(), mainTask));
        // this is the only task that receives messages and it receive from its parent
        if (search.getParent() != null) {
          receiveExecutors.add(plan.getExecutorForChannel(search.getParent().getTaskId()));
          recv.add(search.getParent().getTaskId());
        }
        Map<Integer, List<Integer>> receivePathMap = new HashMap<>();
        receivePathMap.put(0, new ArrayList<>(recv));
        receiveTasks.put(t, receivePathMap);

        // this task is connected to others and they dont send messages to anyone
        List<Integer> directChildren = search.getDirectChildren();
        for (int child : directChildren) {
          Map<Integer, Set<Integer>> sendMap = new HashMap<>();
          Set<Integer> sendTasks = new HashSet<>();
          sendMap.put(MPIContext.DEFAULT_PATH, sendTasks);
          sendInternalTasks.put(child, sendMap);
          destinationIdentifiers.put(t, child);

          Map<Integer, List<Integer>> childReceiveMap = new HashMap<>();
          List<Integer> childReceiveTasks = new ArrayList<>();
          childReceiveTasks.add(t);
          childReceiveMap.put(MPIContext.DEFAULT_PATH, childReceiveTasks);
          receiveTasks.put(child, childReceiveMap);
        }

        // main task is going to send to its internal tasks
        Map<Integer, Set<Integer>> mainInternalSendMap = new HashMap<>();
        Set<Integer> mainInternalSendTasks = new HashSet<>(directChildren);
        mainInternalSendMap.put(MPIContext.DEFAULT_PATH, mainInternalSendTasks);
        sendInternalTasks.put(t, mainInternalSendMap);

        // now lets calculate the external send tasks of the main task
        Map<Integer, Set<Integer>> mainExternalSendMap = new HashMap<>();
        Set<Integer> mainExternalSendTasks = new HashSet<>();
        mainExternalSendTasks.addAll(search.getRemoteChildrenIds());
        mainExternalSendMap.put(MPIContext.DEFAULT_PATH, mainExternalSendTasks);
        sendExternalTasks.put(t, mainExternalSendMap);
        destinationIdentifiers.put(t, 0);
      } else {
        LOG.info(String.format("%d doesn't have a node in tree: %d", plan.getThisExecutor(), t));
      }
    }

    LOG.info(String.format("****** %d send internal tasks: %s",
        plan.getThisExecutor(), sendInternalTasks));
    LOG.info(String.format("****** %d send external tasks: %s",
        plan.getThisExecutor(), sendExternalTasks));
    LOG.info(String.format("****** %d send externalPartial tasks: %s", plan.getThisExecutor(),
        sendExternalTasksPartial));
    LOG.info(String.format("****** %d receive executor: %s",
        plan.getThisExecutor(), receiveExecutors));
    LOG.info(String.format("****** %d receive tasks: %s",
        plan.getThisExecutor(), receiveTasks));
  }

  @Override
  public Set<Integer> receivingExecutors() {
    return receiveExecutors;
  }

  @Override
  public Map<Integer, Map<Integer, List<Integer>>> receiveExpectedTaskIds() {
    return receiveTasks;
  }

  @Override
  public boolean isLastReceiver() {
    // check weather this
    return mainTaskLast;
  }

  @Override
  public Map<Integer, Map<Integer, Set<Integer>>> getInternalSendTasks(int source) {
    return sendInternalTasks;
  }

  public Map<Integer, Map<Integer, Set<Integer>>> getExternalSendTasks(int source) {
    return sendExternalTasks;
  }

  public Map<Integer, Map<Integer, Set<Integer>>> getExternalSendTasksForPartial(int source) {
    return sendExternalTasksPartial;
  }

  @Override
  public int mainTaskOfExecutor(int executor) {
    return mainTask;
  }

  @Override
  public int destinationIdentifier(int source, int path) {
    Object o = destinationIdentifiers.get(source);
    if (o != null) {
      return (int) o;
    } else {
      throw new RuntimeException("Unexpected source requesting destination: " + source);
    }
  }
}
