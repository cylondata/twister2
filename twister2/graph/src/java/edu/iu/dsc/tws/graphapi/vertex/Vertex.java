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
package edu.iu.dsc.tws.graphapi.vertex;


/**
 * Class which holds vertex id, data and edges.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 */
public interface Vertex<I, V, E> {
  /**
   * Initialize id, value, and edges.
   * This method (or the alternative form initialize(id, value)) must be called
   * after instantiation, unless readFields() is called.
   *
   * @param id Vertex id
   * @param value Vertex value
   * @param edges Iterable of edges
   */
  void initialize(I id, V value, Iterable<E> edges);

  /**
   * Initialize id and value. Vertex edges will be empty.
   * This method (or the alternative form initialize(id, value, edges))
   * must be called after instantiation, unless readFields() is called.
   *
   * @param id Vertex id
   * @param value Vertex value
   */
  void initialize(I id, V value);

  /**
   * Get the vertex id.
   *
   * @return My vertex id.
   */
  I getId();

  /**
   * Get the vertex value (data stored with vertex)
   *
   * @return Vertex value
   */
  V getValue();

  /**
   * Set the vertex data (immediately visible in the computation)
   *
   * @param value Vertex data to be set
   */
  void setValue(V value);


  /**
   * Get the number of outgoing edges on this vertex.
   *
   * @return the total number of outbound edges from this vertex
   */
  int getNumEdges();

  /**
   * Get a read-only view of the out-edges of this vertex.
   * Note: edge objects returned by this iterable may be invalidated as soon
   * as the next element is requested. Thus, keeping a reference to an edge
   * almost always leads to undesired behavior.
   * Accessing the edges with other methods (e.g., addEdge()) during iteration
   * leads to undefined behavior.
   *
   * @return the out edges (sort order determined by subclass implementation).
   */
  Iterable<E> getEdges();

  /**
   * Set the outgoing edges for this vertex.
   *
   * @param edges Iterable of edges
   */
  void setEdges(Iterable<E> edges);


  /**
   * Return the value of the first edge with the given target vertex id,
   * or null if there is no such edge.
   * Note: edge value objects returned by this method may be invalidated by
   * the next call. Thus, keeping a reference to an edge value almost always
   * leads to undesired behavior.
   *
   * @param targetVertexId Target vertex id
   * @return Edge value (or null if missing)
   */
  V getEdgeValue(I targetVertexId);

  /**
   * If an edge to the target vertex exists, set it to the given edge value.
   * This only makes sense with strict graphs.
   *
   * @param targetVertexId Target vertex id
   * @param edgeValue Edge value
   */
  void setEdgeValue(I targetVertexId, V edgeValue);


  /**
   * Add an edge for this vertex (happens immediately)
   *
   * @param edge Edge to add
   */
  void addEdge(E edge);

  /**
   * Removes all edges pointing to the given vertex id.
   *
   * @param targetVertexId the target vertex id
   */
  void removeEdges(I targetVertexId);


}

