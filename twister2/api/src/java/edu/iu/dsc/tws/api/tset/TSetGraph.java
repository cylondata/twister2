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
package edu.iu.dsc.tws.api.tset;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import com.google.common.graph.GraphBuilder;
import com.google.common.graph.MutableGraph;

import edu.iu.dsc.tws.api.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.api.task.graph.OperationMode;
import edu.iu.dsc.tws.api.tset.link.BuildableTLink;
import edu.iu.dsc.tws.api.tset.sets.BuildableTSet;
import edu.iu.dsc.tws.api.tset.sets.CacheableTSet;

public class TSetGraph {
  private static final Logger LOG = Logger.getLogger(TSetGraph.class.getName());

  private static int taskGraphCount = 0;

  private TSetEnvironment env;
  private MutableGraph<TBase> graph;
  private edu.iu.dsc.tws.task.graph.GraphBuilder dfwGraphBuilder;
  private OperationMode opMode;

  private Set<BuildableTSet> sources;

  public TSetGraph(TSetEnvironment tSetEnv, OperationMode operationMode) {
    this.env = tSetEnv;
    this.graph = GraphBuilder.directed()
        .allowsSelfLoops(false) // because this is a DAG
        .expectedNodeCount(100000) // use config and change this value
        .build();

    this.opMode = operationMode;

    this.sources = new HashSet<>();

    resetDfwGraphBuilder();
  }

  /**
   * for intermediate nodes. origin --> target
   *
   * @param target target tset
   * @param origin origin tset
   */
  public void addTSet(TBase origin, TBase target) {
    this.graph.putEdge(origin, target);
  }

  /**
   * Specific method to add sources
   *
   * @param source source
   */
  public void addSourceTSet(BuildableTSet source) {
    this.sources.add(source);
    this.graph.addNode(source);
  }

  /**
   * connect two tsets
   *
   * @param target target
   * @param origin origin
   */
  public void connectTSets(TBase origin, TBase target) {
    this.graph.putEdge(origin, target);
  }

  public Set<TBase> getSuccessors(TBase tSet) {
    return this.graph.successors(tSet);
  }

  public Set<TBase> getPredecessors(TBase tSet) {
    return this.graph.predecessors(tSet);
  }

  private boolean removeNode(TBase tSet) {
    return this.graph.removeNode(tSet);
  }

  public Set<TBase> getNodes() {
    return this.graph.nodes();
  }

  public edu.iu.dsc.tws.task.graph.GraphBuilder getDfwGraphBuilder() {
    return dfwGraphBuilder;
  }

  void resetDfwGraphBuilder() {
    this.dfwGraphBuilder = edu.iu.dsc.tws.task.graph.GraphBuilder.newBuilder();
    this.dfwGraphBuilder.operationMode(opMode);
  }

  /**
   * Builds the entire graph
   * todo: this is performing iterative BFS. Use multi source BFS instead
   *
   * @return data flow graph to execute
   */
  public DataFlowTaskGraph build() {
    List<BuildableTLink> links = new ArrayList<>();
    Set<BuildableTSet> sets = new HashSet<>();

    List<TBase> buildOrder = new ArrayList<>();

    for (BuildableTSet src : sources) {
      buildOrder.addAll(bfs(src, links, sets, this::getSuccessors));
    }

    LOG.info(() -> "Build order: " + buildOrder.toString());

    return buildGraph(links, sets);
  }

  /**
   * Builds a subgraph of TSets from the specified TSet
   *
   * @param leafTSet leaf tset
   * @return data flow graph to execute the subgraph of TSets
   */
  public DataFlowTaskGraph build(BuildableTSet leafTSet) {
    List<BuildableTLink> links = new ArrayList<>();
    Set<BuildableTSet> sets = new HashSet<>();

    List<TBase> buildOrder = bfs(leafTSet, links, sets, this::getPredecessors);

    LOG.info(() -> "Build order: " + buildOrder.toString());

    return buildGraph(links, sets);
  }

  private DataFlowTaskGraph buildGraph(List<BuildableTLink> links, Collection<BuildableTSet> sets) {

    LOG.fine(() -> "Node build plan: " + sets);
    for (BuildableTSet baseTSet : sets) {
      baseTSet.build(this);
    }

    LOG.fine(() -> "Edge build plan: " + links);
    // links need to be built in order. check issue #519
    for (int i = 0; i < links.size(); i++) {
      links.get(links.size() - i - 1).build(this, sets);
    }

    DataFlowTaskGraph dataflowGraph = getDfwGraphBuilder().build();
    dataflowGraph.setGraphName("taskgraph" + (++taskGraphCount));

    // clean the upstream of the cached tsets
    if (cleanUpstream(sets)) {
      LOG.info("Some TSets have been cleaned up!");
    }

    return dataflowGraph;
  }

  private List<TBase> bfs(BuildableTSet s, Collection<BuildableTLink> links,
                          Collection<BuildableTSet> sets, AdjNodesExtractor adjNodesExtractor) {
    List<TBase> buildOrder = new ArrayList<>();

    Map<TBase, Boolean> visited = new HashMap<>();

    Deque<TBase> queue = new LinkedList<>();

    visited.put(s, true);
    queue.add(s);

    while (queue.size() != 0) {
      TBase t = queue.poll();
      buildOrder.add(t);
      if (t instanceof BuildableTLink) {
        links.add((BuildableTLink) t);
      } else if (t instanceof BuildableTSet) {
        sets.add((BuildableTSet) t);
      }

      for (TBase parent : adjNodesExtractor.extract(t)) {
        if (visited.get(parent) == null || !visited.get(parent)) {
          visited.put(parent, true);
          queue.add(parent);
        }
      }
    }

    Collections.reverse(buildOrder);

    return buildOrder;
  }

  private boolean cleanUpstream(Collection<BuildableTSet> tSets) {
    Set<TBase> toRemove = new HashSet<>();

    boolean changed = false;

    for (BuildableTSet tset : tSets) {
      if (tset instanceof CacheableTSet) {
        toRemove.addAll(getPredecessors(tset));
      }
    }

    for (TBase tset : toRemove) {
      changed = changed || removeNode(tset);
    }

    return changed;
  }

  interface AdjNodesExtractor {
    Set<TBase> extract(TBase node);
  }
}
