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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.mpi.MPIContext;

public class BinaryTreeRouter implements IRouter {
  private static final Logger LOG = Logger.getLogger(BinaryTreeRouter.class.getName());

  private Config config;
  private TaskPlan taskPlan;
  private Set<Integer> sources;
  private Set<Integer> destinations;
  private int stream;
  private int intraNodeDegree;
  private int interNodeDegree;
  private int distinctRoutes;
  private BinaryTree tree;

  /**
   * Initialize the data structure
   *
   * @param cfg
   * @param plan
   * @param srscs
   * @param dests
   * @param strm
   */
  public BinaryTreeRouter(Config cfg, TaskPlan plan,
                   Set<Integer> srscs, Set<Integer> dests, int strm, int distinctRts) {
    this.config = cfg;
    this.taskPlan = plan;
    this.sources = srscs;
    this.destinations = dests;
    this.stream = strm;
    this.distinctRoutes = distinctRts;

    this.interNodeDegree = MPIContext.interNodeDegree(cfg, 2);
    this.intraNodeDegree = MPIContext.intraNodeDegree(cfg, 2);

    tree = new BinaryTree(interNodeDegree, intraNodeDegree, taskPlan, 0, dests);

    calculateRoutingTable();
  }

  private void calculateRoutingTable() {
    ArrayList<Integer> sourceList = new ArrayList<>(sources);
    Collections.sort(sourceList);

    // we can only have max routes equal to sources
    int routs = Math.min(Math.min(distinctRoutes, sourceList.size()), destinations.size());

    for (int i = 0; i < sourceList.size(); i++) {
      int source = sourceList.get(i);
      int index = i % routs;

//      Node root = buildInterGroupTree(index);
      Node root = null;
      Node search = BinaryTree.search(root, 0);
      if (search != null) {
        Routing routing = getRouting(search);
        if (routing != null) {
          // expectedRouting.put(source, routing);
          throw new RuntimeException("");
        }
      }
    }
  }

  private Routing getRouting(Node node) {
    List<Integer> upstream = new ArrayList<>();
    List<Integer> downstrean = new ArrayList<>();

    Node parent = node.getParent();
    upstream.add(parent.getTaskId());

    List<Node> children = node.getChildren();
    for (Node child : children) {
      downstrean.add(child.getTaskId());
    }
    downstrean.addAll(node.getDirectChildren());
    return new Routing(upstream, downstrean);
  }

  @Override
  public int mainTaskOfExecutor(int executor) {
    return 0;
  }

  @Override
  public int destinationIdentifier(int source, int path) {
    return 0;
  }

  @Override
  public Set<Integer> receivingExecutors() {
    return null;
  }

  @Override
  public Map<Integer, Map<Integer, List<Integer>>>  receiveExpectedTaskIds() {
    return null;
  }

  @Override
  public boolean isLastReceiver() {
    return false;
  }

  @Override
  public Map<Integer, Map<Integer, Set<Integer>>> getInternalSendTasks(int src) {
    return null;
  }

  @Override
  public Map<Integer, Map<Integer, Set<Integer>>> getExternalSendTasks(int source) {
    return null;
  }
}
