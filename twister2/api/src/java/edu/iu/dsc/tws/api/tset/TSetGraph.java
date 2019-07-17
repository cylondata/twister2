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

import java.util.Set;

import com.google.common.graph.GraphBuilder;
import com.google.common.graph.MutableGraph;

import edu.iu.dsc.tws.api.task.graph.OperationMode;

public class TSetGraph {

  private TSetEnvironment env;

  private MutableGraph<TBase> graph;

  private edu.iu.dsc.tws.task.graph.GraphBuilder dfwGraphBuilder;

  public TSetGraph(TSetEnvironment tSetEnv, OperationMode operationMode) {
    this.env = tSetEnv;
    this.graph = GraphBuilder.directed()
        .allowsSelfLoops(false) // because this is a DAG
        .expectedNodeCount(100000) // use config and change this value
        .build();

    this.dfwGraphBuilder = edu.iu.dsc.tws.task.graph.GraphBuilder.newBuilder();
    this.dfwGraphBuilder.operationMode(operationMode);
  }

  /**
   * for intermediate nodes. u --> v
   *
   * @param v child tset
   * @param u parent tset
   */
  public void addTSet(TBase u, TBase v) {
    this.graph.putEdge(u, v);
  }

  /**
   * for sources and sinks
   *
   * @param u source/ sink
   */
  public void addTSet(TBase u) {
    this.graph.addNode(u);
  }

  /**
   * connect two tsets
   *
   * @param v v
   * @param u u
   */
  public void connectTSets(TBase u, TBase v) {
    this.graph.putEdge(u, v);
  }

  public Set<TBase> getSuccessors(TBase tSet) {
    return this.graph.successors(tSet);
  }

  public Set<TBase> getPredecessors(TBase tSet) {
    return this.graph.predecessors(tSet);
  }

  public Set<TBase> getNodes() {
    return this.graph.nodes();
  }

  public edu.iu.dsc.tws.task.graph.GraphBuilder getDfwGraphBuilder() {
    return dfwGraphBuilder;
  }

}
