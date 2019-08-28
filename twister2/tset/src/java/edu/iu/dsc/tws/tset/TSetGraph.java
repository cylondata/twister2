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
package edu.iu.dsc.tws.tset;

import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import com.google.common.graph.ElementOrder;
import com.google.common.graph.GraphBuilder;
import com.google.common.graph.MutableGraph;

import edu.iu.dsc.tws.api.compute.graph.ComputeGraph;
import edu.iu.dsc.tws.api.compute.graph.OperationMode;
import edu.iu.dsc.tws.api.tset.Cacheable;
import edu.iu.dsc.tws.api.tset.TBase;
import edu.iu.dsc.tws.tset.env.TSetEnvironment;
import edu.iu.dsc.tws.tset.links.BuildableTLink;
import edu.iu.dsc.tws.tset.sets.BuildableTSet;

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
        .nodeOrder(ElementOrder.insertion())
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
    if (nodeNotExists(origin)) {
      this.graph.addNode(origin);
    }

    if (nodeNotExists(target)) {
      this.graph.addNode(target);
    }

    this.graph.putEdge(origin, target);
  }

  /**
   * Specific method to add sources
   *
   * @param source source
   */
  public void addSourceTSet(BuildableTSet source) {
    if (nodeNotExists(source)) {
      this.sources.add(source);
      this.graph.addNode(source);
    }
  }

  /**
   * connect two tsets
   *
   * @param target target
   * @param origin origin
   */
  public void connectTSets(TBase origin, TBase target) {
    this.addTSet(origin, target);
  }

  private boolean nodeNotExists(TBase tBase) {
    return !this.graph.nodes().contains(tBase);
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

  public void resetDfwGraphBuilder() {
    this.dfwGraphBuilder = edu.iu.dsc.tws.task.graph.GraphBuilder.newBuilder();
    this.dfwGraphBuilder.operationMode(opMode);
  }

  /**
   * Builds the entire graph
   * todo: this is performing iterative BFS. Use multi source BFS instead
   *
   * @return data flow graph to execute
   */
  public ComputeGraph build() {
    Set<BuildableTLink> links = new LinkedHashSet<>();
    Set<BuildableTSet> sets = new LinkedHashSet<>();

    Set<TBase> buildOrder = new LinkedHashSet<>();

    for (BuildableTSet src : sources) {
      buildOrder.addAll(bfs(src, links, sets, this::getSuccessors));
    }

    LOG.info(() -> "Build order: " + buildOrder.toString());

    return buildGraph(links, sets, false);
  }

  /**
   * Builds a subgraph of TSets from the specified TSet
   *
   * @param leafTSet leaf tset
   * @return data flow graph to execute the subgraph of TSets
   */
  public ComputeGraph build(BuildableTSet leafTSet) {
    Set<BuildableTLink> links = new LinkedHashSet<>();
    Set<BuildableTSet> sets = new LinkedHashSet<>();

    Set<TBase> buildOrder = bfs(leafTSet, links, sets, this::getPredecessors);

    LOG.info(() -> "Build order: " + buildOrder.toString());

    return buildGraph(links, sets, true);
  }

  private ComputeGraph buildGraph(Collection<BuildableTLink> links, Collection<BuildableTSet> sets,
                                  boolean reverse) {

    LOG.info(() -> "Node build order: " + sets + " reversed: " + reverse);
    Iterator<BuildableTSet> setsItr = reverse ? new LinkedList<>(sets).descendingIterator()
        : sets.iterator();
    while (setsItr.hasNext()) {
      setsItr.next().build(this);
    }

    LOG.info(() -> "Edge build order: " + links + " reversed: " + reverse);
    // links need to be built in order. check issue #519
/*    for (int i = 0; i < links.size(); i++) {
      links.get(links.size() - i - 1).build(this, sets);
    }*/
    Iterator<BuildableTLink> linksItr = reverse ? new LinkedList<>(links).descendingIterator()
        : links.iterator();
    while (linksItr.hasNext()) {
      linksItr.next().build(this, sets);
    }

    ComputeGraph dataflowGraph = getDfwGraphBuilder().build();
    dataflowGraph.setGraphName("taskgraph" + (++taskGraphCount));

    // clean the upstream of the cached tsets
    if (cleanUpstream(sets)) {
      LOG.info("Some TSets have been cleaned up!");
    }

    return dataflowGraph;
  }

  private Set<TBase> bfs(BuildableTSet s, Collection<BuildableTLink> links,
                         Collection<BuildableTSet> sets, AdjNodesExtractor adjNodesExtractor) {
    Set<TBase> buildOrder = new LinkedHashSet<>();

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

    return buildOrder;
  }

  private boolean cleanUpstream(Collection<BuildableTSet> tSets) {
    Set<TBase> toRemove = new HashSet<>();

    boolean changed = false;

    for (BuildableTSet tset : tSets) {
      if (tset instanceof Cacheable) {
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
