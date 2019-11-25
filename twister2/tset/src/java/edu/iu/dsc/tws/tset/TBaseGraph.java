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
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.compute.graph.OperationMode;
import edu.iu.dsc.tws.api.tset.TBase;
import edu.iu.dsc.tws.api.tset.link.TLink;
import edu.iu.dsc.tws.tset.env.BuildContext;
import edu.iu.dsc.tws.tset.graph.DAGMutableGraph;
import edu.iu.dsc.tws.tset.graph.MutableGraph;
import edu.iu.dsc.tws.tset.sets.BuildableTSet;

public class TBaseGraph {
  private static final Logger LOG = Logger.getLogger(TBaseGraph.class.getName());

  private MutableGraph<TBase> graph;
  //  private edu.iu.dsc.tws.task.graph.GraphBuilder dfwGraphBuilder;
  private OperationMode opMode;

  private Set<BuildableTSet> sources;

  public TBaseGraph(OperationMode operationMode) {
//    this.graph = GraphBuilder.directed()
//        .allowsSelfLoops(false) // because this is a DAG
//        .expectedNodeCount(100000) // use config and change this value
//        .nodeOrder(ElementOrder.insertion())
//        .build();
    this.graph = new DAGMutableGraph<>();

    this.opMode = operationMode;

    this.sources = new HashSet<>();
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
    Set<TBase> res = this.graph.successors(tSet);
    return res != null ? res : Collections.emptySet();
  }

  public Set<TBase> getPredecessors(TBase tSet) {
    Set<TBase> res = this.graph.predecessors(tSet);
    return res != null ? res : Collections.emptySet();
  }

  private boolean removeNode(TBase tSet) {
    return this.graph.removeNode(tSet);
  }

  public Set<TBase> getNodes() {
    return this.graph.nodes();
  }

  private BuildContext doBuild(Set<BuildableTSet> roots, AdjNodesExtractor nodesExtractor) {
    String buildId = TSetUtils.generateBuildId(roots);

    Set<TBase> buildSeq = conditionalBFS(roots, nodesExtractor);
    LOG.log(Level.FINE, () -> "Build order for " + buildId + " : " + buildSeq.toString());

    return new BuildContext(buildId, roots, buildSeq, opMode);
  }

  /**
   * Builds only one TSet (NOT the subgraph)
   *
   * @param tSet TSet to build
   * @return build context
   */
  public BuildContext buildOne(BuildableTSet tSet) {
    String buildId = TSetUtils.generateBuildId(tSet);

    LOG.info(() -> "Build order for " + buildId + " : " + tSet.toString());

    return new BuildContext(buildId, Collections.singleton(tSet), Collections.singleton(tSet),
        opMode);
  }

  /**
   * Builds the entire graph
   *
   * @return data flow graph to execute
   */
  public BuildContext build() {
    // when building the entire graph, we will be going top-down direction. Hence getSuccessors
    return doBuild(sources, this::getSuccessors);
  }

  /**
   * Builds a subgraph of TSets from the specified TSet
   *
   * @param leafTSet leaf tset
   * @return data flow graph to execute the subgraph of TSets
   */
  public BuildContext build(BuildableTSet leafTSet) {
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
      }
    }

    return buildSequence;
  }

  interface AdjNodesExtractor {
    Set<TBase> extract(TBase node);
  }

  private boolean allAdjNodesTraversed(TLink node, Set<TBase> buildSeq,
                                       AdjNodesExtractor adjNodesExtractor) {
    // all adj nodes needs to be in the build sequence
    for (TBase adj : adjNodesExtractor.extract(node)) {
      if (!buildSeq.contains(adj)) {
        return false;
      }
    }
    return true;
  }
}
