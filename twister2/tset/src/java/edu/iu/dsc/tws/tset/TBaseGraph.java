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
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.StringJoiner;
import java.util.logging.Logger;

import com.google.common.graph.ElementOrder;
import com.google.common.graph.GraphBuilder;
import com.google.common.graph.MutableGraph;

import edu.iu.dsc.tws.api.compute.graph.ComputeGraph;
import edu.iu.dsc.tws.api.compute.graph.OperationMode;
import edu.iu.dsc.tws.api.tset.Cacheable;
import edu.iu.dsc.tws.api.tset.TBase;
import edu.iu.dsc.tws.api.tset.link.TLink;
import edu.iu.dsc.tws.tset.env.TBaseBuildContext;
import edu.iu.dsc.tws.tset.sets.BuildableTSet;

public class TBaseGraph {
  private static final Logger LOG = Logger.getLogger(TBaseGraph.class.getName());

  private MutableGraph<TBase> graph;
  private edu.iu.dsc.tws.task.graph.GraphBuilder dfwGraphBuilder;
  private OperationMode opMode;

  private Set<BuildableTSet> sources;

  public TBaseGraph(OperationMode operationMode) {
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

  private TBaseBuildContext doBuild(Set<BuildableTSet> roots, AdjNodesExtractor nodesExtractor) {
    String buildId = generateBuildId(roots);

    Set<TBase> buildSeq = conditionalBFS(roots, nodesExtractor);
    LOG.info(() -> "Build order for build " + buildId + " : " + buildSeq.toString());

    // building the individual TBases
    for (TBase node : buildSeq) {
      // here, build seq is required for tlinks to filter out nodes that are relevant to this
      // particular build sequence
      ((Buildable) node).build(buildSeq);
    }

    ComputeGraph dataflowGraph = getDfwGraphBuilder().build();
    dataflowGraph.setGraphName(buildId);

    return new TBaseBuildContext(buildId, sources, buildSeq, dataflowGraph);
  }

  /**
   * Builds the entire graph
   *
   * @return data flow graph to execute
   */
  public TBaseBuildContext build() {
    // when building the entire graph, we will be going top-down direction. Hence getSuccessors
    return doBuild(sources, this::getSuccessors);
  }

  /**
   * Builds a subgraph of TSets from the specified TSet
   *
   * @param leafTSet leaf tset
   * @return data flow graph to execute the subgraph of TSets
   */
  public TBaseBuildContext build(BuildableTSet leafTSet) {
    // when building a TSet related to an action, we will be going bottom-up direction.
    // Hence getPredecessors
    return doBuild(Collections.singleton(leafTSet), this::getPredecessors);
  }

  /**
   * This is a tweaked version of BFS, where a node would be skipped from processing and added back
   * to the queue, if a particular condition is met.
   * Here, the condition is, if the current TBase is a TLink, then check if all its adjacent nodes
   * have been traversed. If these conditions are met, process the tlink, else add it back to the
   * end of the queue
   *
   * @return build seq
   */
  private Set<TBase> conditionalBFS(BuildableTSet root, AdjNodesExtractor adjNodesExtractor) {
    return conditionalBFS(Collections.singletonList(root), adjNodesExtractor);
  }

  /**
   * Same case as above this but can handle multiple roots
   *
   * @return build seq
   */
  private Set<TBase> conditionalBFS(Collection<? extends BuildableTSet> roots,
                                    AdjNodesExtractor adjNodesExtractor) {
    Set<TBase> buildSequence = new LinkedHashSet<>();
    Set<TBase> visited = new HashSet<>();
    Deque<TBase> queue = new LinkedList<>();

    for (TBase root : roots) {
      visited.add(root);
      queue.add(root);

      while (queue.size() != 0) {
        TBase t = queue.poll();

        for (TBase adj : adjNodesExtractor.extract(t)) {
          if (!visited.contains(adj)) {
            visited.add(adj);
            queue.add(adj);
          }
        }

        if (t instanceof TLink && !allAdjNodesTraversed((TLink) t, buildSequence,
            adjNodesExtractor)) {
          queue.add(t);
          continue;
        }

        buildSequence.add(t);

        // Following was wrong because, when running BFS on the subsequent roots, if there are
        // join/ union edges, those tlinks have already been built. so, buildSquence needs to be
        // built separately

        // here it is sufficient to pass the partial build sequence to build the links. Because the
        // above condition ensures that all the relevant nodes at both ends of a tlink are built,
        // before building the TLink.
//        ((Buildable) t).build(buildSequence);
      }
    }

    return buildSequence;
  }

  // todo: this functionality is broken!!
  private boolean cleanUpstream(Collection<BuildableTSet> tSets) {
    Set<TBase> toRemove = new HashSet<>();

    boolean changed = false;

    // todo: need to clean up the entire upstream! not just the precessesor of cacheable!!
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

  private boolean allAdjNodesTraversed(TLink node, Set<TBase> buildSeq,
                                       AdjNodesExtractor adjNodesExtractor) {
    // all adj nodes needs to be in the build sequence
    for (TBase adj : adjNodesExtractor.extract((TBase) node)) {
      if (!buildSeq.contains(adj)) {
        return false;
      }
    }
    return true;
  }

  private static String generateBuildId(Set<? extends TBase> roots) {
    StringJoiner joiner = new StringJoiner("_");
    joiner.add("build");
    for (TBase t : roots) {
      joiner.add(t.getId());
    }
    return joiner.toString();
  }

  private static String generateBuildId(TBase root) {
    return "build_" + root.getId();
  }
}
