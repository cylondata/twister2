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
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.dfw.DataFlowContext;
import edu.iu.dsc.tws.comms.utils.TaskPlanUtils;

public class KeyedInvertedBinaryTreeRouter {
  private static final Logger LOG = Logger.getLogger(KeyedInvertedBinaryTreeRouter.class.getName());
  private TaskPlan taskPlan;
  // source -> Path -> task
  private Map<Integer, Map<Integer, List<Integer>>> receiveTasks;
  // the executors we are receiving from
  private Set<Integer> receiveExecutors;
  // source -> Path -> task
  private Map<Integer, Map<Integer, Set<Integer>>> sendExternalTasksPartial;
  // source -> Path -> task
  private Map<Integer, Map<Integer, Set<Integer>>> sendExternalTasks;
  // source -> Path -> task
  private Map<Integer, Map<Integer, Set<Integer>>> sendInternalTasks;
  // path -> main task
  private Map<Integer, Integer> mainTask;
  private boolean mainTaskLast;
  // path -> source -> destination
  private Map<Integer, Map<Integer, Integer>> destinationIdentifiers;
  // the last task assigned to path path -> task
  private Map<Integer, Integer> pathToTask;

  /**
   * Initialize the data structure
   *
   * @param cfg
   * @param plan
   * @param roots
   * @param dests
   */
  public KeyedInvertedBinaryTreeRouter(Config cfg, TaskPlan plan,
                                  Set<Integer> roots, Set<Integer> dests) {
    int interNodeDegree = DataFlowContext.interNodeDegree(cfg, 2);
    int intraNodeDegree = DataFlowContext.intraNodeDegree(cfg, 2);
    mainTaskLast = false;
    this.taskPlan = plan;
    this.destinationIdentifiers = new HashMap<>();
    this.pathToTask = new HashMap<>();
    // construct the map of receiving ids
    this.receiveTasks = new HashMap<Integer, Map<Integer, List<Integer>>>();
    // now lets construct the downstream tasks
    sendExternalTasksPartial = new HashMap<>();
    sendInternalTasks = new HashMap<>();
    sendExternalTasks = new HashMap<>();
    receiveExecutors = new HashSet<>();
    List<Integer> rootsSorted = new ArrayList<Integer>(roots);
    Collections.sort(rootsSorted);
    mainTask = new HashMap<>();

    Set<Integer> destinationsInThisExecutor = TaskPlanUtils.getTasksOfThisWorker(plan, dests);
    for (int t : destinationsInThisExecutor) {
      int path = rootsSorted.indexOf(t);
      pathToTask.put(path, t);
    }

    for (int path = 0; path < roots.size(); path++) {
      int root = rootsSorted.get(path);
      // lets build the tree
      BinaryTree tree = new BinaryTree(interNodeDegree, intraNodeDegree, plan,
          root, dests);
      Node treeRoot = tree.buildInterGroupTree(path);

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

      // now lets construct the receive tasks tasks
      for (int t : thisExecutorTasksOfOperation) {
        List<Integer> recv = new ArrayList<>();

        Node search = BinaryTree.search(treeRoot, t);
        // okay this is the main task of this executor
        if (search != null) {
          mainTask.put(path, search.getTaskId());
          LOG.fine(String.format("%d main task: %s", plan.getThisExecutor(), mainTask));
          // this is the only task that receives messages
          for (int k : search.getRemoteChildrenIds()) {
            receiveExecutors.add(plan.getExecutorForChannel(k));
          }
          recv.addAll(search.getAllChildrenIds());

          // check weather we have this map created before
          Map<Integer, List<Integer>> receivePathMap;
          if (receiveTasks.containsKey(t)) {
            receivePathMap = receiveTasks.get(t);
          } else {
            receivePathMap = new HashMap<>();
          }
          receivePathMap.put(path, new ArrayList<>(recv));
          receiveTasks.put(t, receivePathMap);

          // this task is connected to others and they send the message to this task
          List<Integer> directChildren = search.getDirectChildren();

          if (t == path) {
            LOG.log(Level.FINE, String.format("%d direct children %s",
                plan.getThisExecutor(), directChildren));
          }

          for (int child : directChildren) {
            Map<Integer, Set<Integer>> sendMap;
            if (sendInternalTasks.containsKey(child)) {
              sendMap = sendInternalTasks.get(child);
            } else {
              sendMap = new HashMap<>();
            }
            Set<Integer> sendTasks = new HashSet<>();
            sendTasks.add(t);
            sendMap.put(path, sendTasks);
            sendInternalTasks.put(child, sendMap);

            Map<Integer, Integer> destinationMap = new HashMap<>();
            if (destinationIdentifiers.containsKey(path)) {
              destinationMap = destinationIdentifiers.get(path);
            }
            destinationMap.put(child, t);
            destinationIdentifiers.put(path, destinationMap);
          }

          // now lets calculate the external send tasks of the main task
          Node parent = search.getParent();
          if (parent != null) {
            Map<Integer, Set<Integer>> mainSendMap;
            if (sendExternalTasksPartial.containsKey(t)) {
              mainSendMap = sendExternalTasksPartial.get(t);
            } else {
              mainSendMap = new HashMap<>();
            }
            Set<Integer> sendTasks = new HashSet<>();
            sendTasks.add(parent.getTaskId());
            mainSendMap.put(path, sendTasks);
            sendExternalTasksPartial.put(t, mainSendMap);

            Map<Integer, Integer> destinationMap = new HashMap<>();
            if (destinationIdentifiers.containsKey(path)) {
              destinationMap = destinationIdentifiers.get(path);
            }
            destinationMap.put(t, parent.getTaskId());
            destinationIdentifiers.put(path, destinationMap);
          } else {
            mainTaskLast = true;
          }
        } else {
          LOG.fine(String.format("%d doesn't have a node in tree: %d", plan.getThisExecutor(), t));
        }
      }
    }

//    LOG.info(String.format("****** %d internal tasks: %s",
//        plan.getThisExecutor(), sendInternalTasks));
//    LOG.info(String.format("****** %d external tasks: %s",
//        plan.getThisExecutor(), sendExternalTasks));
//    LOG.info(String.format("****** %d externalPartial tasks: %s", plan.getThisExecutor(),
//        sendExternalTasksPartial));
//    LOG.info(String.format("****** %d receive executor: %s",
//        plan.getThisExecutor(), receiveExecutors));
//    LOG.info(String.format("****** %d receive tasks: %s",
//        plan.getThisExecutor(), receiveTasks));
//    LOG.info(String.format("****** %d main tasks: %s", plan.getThisExecutor(), mainTask));
  }

  public Set<Integer> receivingExecutors() {
    return receiveExecutors;
  }

  public Map<Integer, Map<Integer, List<Integer>>> receiveExpectedTaskIds() {
    return receiveTasks;
  }

  public boolean isLastReceiver() {
    // check weather this
    return mainTaskLast;
  }

  public Map<Integer, Map<Integer, Set<Integer>>> getInternalSendTasks(int source) {
    return sendInternalTasks;
  }

  public Map<Integer, Map<Integer, Set<Integer>>> getExternalSendTasks(int source) {
    return sendExternalTasks;
  }

  public Map<Integer, Map<Integer, Set<Integer>>> getExternalSendTasksForPartial(int source) {
    return sendExternalTasksPartial;
  }

  public int mainTaskOfExecutor(int executor, int path) {
    if (mainTask.containsKey(path)) {
      return mainTask.get(path);
    } else {
      throw new RuntimeException(String.format("%d Requesting an path that doesn't exist: %d",
          taskPlan.getThisExecutor(), path));
    }
  }

  public int destinationIdentifier(int source, int path) {
    Map<Integer, Integer> o = destinationIdentifiers.get(path);
    if (o != null) {
      Object dest = o.get(source);
      if (dest != null) {
        return (int) dest;
      } else {
        throw new RuntimeException("Unexpected source and path requesting destination");
      }
    } else {
      throw new RuntimeException("Unexpected source requesting destination: " + source);
    }
  }

  public Map<Integer, Integer> getPathAssignedToTasks() {
    return pathToTask;
  }
}
