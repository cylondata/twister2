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

import java.io.Serializable;
import java.util.Set;

/**
 * Graph used for modeling the TSet dependency structure
 */
public interface MutableGraph<T> extends Serializable {

  /**
   * Adds {@code node} if it is not already present.
   *
   * <p><b>Nodes must be unique</b>, just as {@code Map} keys must be. They must also be non-null.
   *
   * @return {@code true} if the graph was modified as a result of this call
   */
  boolean addNode(T node);

  /**
   * Adds an edge connecting {@code nodeU} to {@code nodeV} if one is not already present.
   *
   * <p>If the graph is directed, the resultant edge will be directed; otherwise, it will be
   * undirected.
   *
   * <p>If {@code nodeU} and {@code nodeV} are not already present in this graph, this method will
   * silently {@link #addNode(Object) add} {@code nodeU} and {@code nodeV} to the graph.
   *
   * @return {@code true} if the graph was modified as a result of this call
   * @throws IllegalArgumentException if the introduction of the edge would violate {@link
   * #allowsSelfLoops()}
   */
  boolean putEdge(T orgin, T target);

  Set<T> nodes();

  /**
   * Removes {@code node} if it is present; all edges incident to {@code node} will also be removed.
   *
   * @return {@code true} if the graph was modified as a result of this call
   */
  boolean removeNode(T node);


  /**
   * Removes the edge connecting {@code nodeU} to {@code nodeV}, if it is present.
   *
   * @return {@code true} if the graph was modified as a result of this call
   */
  boolean removeEdge(T nodeU, T nodeV);

  /**
   * Returns all nodes in this graph adjacent to {@code node} which can be reached by traversing
   * {@code node}'s outgoing edges in the direction (if any) of the edge.
   *
   * @throws IllegalArgumentException if {@code node} is not an element of this graph
   */
  Set<T> successors(T node);


  /**
   * Returns all nodes in this graph adjacent to {@code node} which can be reached by traversing
   * {@code node}'s incoming edges <i>against</i> the direction (if any) of the edge.
   *
   * @throws IllegalArgumentException if {@code node} is not an element of this graph
   */
  Set<T> predecessors(T node);

  /**
   * Returns true if this graph allows self-loops (edges that connect a node to itself). Attempting
   * to add a self-loop to a graph that does not allow them will throw an {@link
   * IllegalArgumentException}.
   */
  default boolean allowsSelfLoops() {
    return false;
  }

  /**
   * Returns true if the edges in this graph are directed.
   */
  boolean isDirected();

  /**
   * returns the node which has the given Id or null if no such node exsists
   * @param id the id of the node to return
   */
  T getNodeById(String id);
}
