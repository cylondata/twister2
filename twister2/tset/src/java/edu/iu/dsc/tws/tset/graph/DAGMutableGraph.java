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
package edu.iu.dsc.tws.tset.graph;

import java.util.Set;

/**
 * DAG graph implementation
 * @param <T> node type
 */
public class DAGMutableGraph<T> implements MutableGraph<T> {

  final boolean directed;

  public DAGMutableGraph(boolean isDirected) {
    this.directed = isDirected;
  }

  @Override
  public boolean addNode(T node) {
    return false;
  }

  @Override
  public boolean putEdge(T orgin, T target) {
    return false;
  }

  @Override
  public Set<T> nodes() {
    return null;
  }

  @Override
  public boolean removeNode(T node) {
    return false;
  }

  @Override
  public boolean removeEdge(T nodeU, T nodeV) {
    return false;
  }

  @Override
  public T successors(T node) {
    return null;
  }

  @Override
  public T predecessors(T node) {
    return null;
  }

  @Override
  public boolean isDirected() {
    return directed;
  }
}
