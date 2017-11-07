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

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.mpi.MPIContext;

public class SingleTargetBinaryTreeRouter implements IRouter {
  private Config config;
  private TaskPlan taskPlan;
  private int sources;
  private Set<Integer> destinations;
  private int stream;
  private int task;
  private int intraNodeDegree;
  private int interNodeDegree;
  private int distinctRoutes;
  private BinaryTree tree;
  private Node treeRoot;
  private Map<Integer, Map<Integer, List<Integer>>> upstream;
  private Set<Integer> receiveExecutors;
  private Map<Integer, Map<Integer, Set<Integer>>> sendExternalTasks;
  private Map<Integer, Map<Integer, Set<Integer>>> sendInternalTasks;

  /**
   * Tasks belonging to this operation and in the same executor
   */
  private Set<Integer> thisExecutorTasksOfOperation;

  /**
   * Initialize the data structure
   *
   * @param cfg
   * @param plan
   * @param root
   * @param dests
   * @param strm
   */
  public SingleTargetBinaryTreeRouter(Config cfg, TaskPlan plan,
                          int root, Set<Integer> dests, int strm, int distinctRts) {
    this.config = cfg;
    this.taskPlan = plan;
    this.sources = root;
    this.destinations = dests;
    this.stream = strm;
    this.distinctRoutes = distinctRts;

    this.interNodeDegree = MPIContext.interNodeDegree(cfg, 2);
    this.intraNodeDegree = MPIContext.intraNodeDegree(cfg, 2);

    // lets build the tree
    tree = new BinaryTree(interNodeDegree, intraNodeDegree, taskPlan, root, dests);
    treeRoot = tree.buildInterGroupTree(0);

    Set<Integer> thisExecutorTasks = taskPlan.getChannelsOfExecutor(taskPlan.getThisExecutor());
    thisExecutorTasksOfOperation = new HashSet<>();
    for (int t : thisExecutorTasks) {
      if (dests.contains(t) || root == t) {
        thisExecutorTasksOfOperation.add(t);
      }
    }

    // construct the map of receiving ids
    this.upstream = new HashMap<Integer, Map<Integer, List<Integer>>>();

    // now lets construct the downstream tasks
    sendExternalTasks = new HashMap<>();
    // now lets construct the receive tasks tasks
    receiveExecutors = new HashSet<>();
    for (int t : thisExecutorTasksOfOperation) {
      List<Integer> recv = new ArrayList<>();
      Node search = BinaryTree.search(treeRoot, t);
      if (search == null) {
        // we do no have the tasks that are directly connected to the tree node
        continue;
      }
      receiveExecutors.addAll(search.getRemoteChildrenIds());
      recv.addAll(search.getAllChildrenIds());

      Map<Integer, List<Integer>> receivePathMap = new HashMap<>();
      receivePathMap.put(0, new ArrayList<>(recv));
      upstream.put(t, receivePathMap);

      Map<Integer, Set<Integer>> sendMap = new HashMap<>();

      Node parent = search.getParent();
      if (parent != null) {
        Set<Integer> sendTasks = new HashSet<>();
        sendTasks.add(parent.getTaskId());
        sendMap.put(t, sendTasks);
        if (thisExecutorTasksOfOperation.contains(parent.getTaskId())) {
          sendInternalTasks.put(t, sendMap);
        } else {
          sendExternalTasks.put(t, sendMap);
        }
      }
    }


  }

  @Override
  public Set<Integer> receivingExecutors() {
    return receiveExecutors;
  }

  @Override
  public Map<Integer, Map<Integer, List<Integer>>> receiveExpectedTaskIds() {
    return upstream;
  }

  @Override
  public boolean isLast(int t) {
    // check weather this
    return true;
  }

  @Override
  public Map<Integer, Map<Integer, Set<Integer>>> getInternalSendTasks(int source) {
    return sendInternalTasks;
  }

  public Map<Integer, Map<Integer, Set<Integer>>> getExternalSendTasks(int source) {
    return sendExternalTasks;
  }

  @Override
  public int mainTaskOfExecutor(int executor) {
    return 0;
  }

  @Override
  public int destinationIdentifier() {
    return 0;
  }
}
