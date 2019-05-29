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
package edu.iu.dsc.tws.graphapi.computation;

import java.io.IOException;
import java.util.Iterator;

import edu.iu.dsc.tws.graphapi.edge.Edge;
import edu.iu.dsc.tws.graphapi.edge.OutEdges;
import edu.iu.dsc.tws.graphapi.vertex.Vertex;

/**
 * Interface for an application for computation.
 *
 * During the superstep there can be several instances of this interface,
 * each doing computation on one partition of the graph's vertices.
 *
 * Note that each thread will have its own {@link Computation},
 * so accessing any data from this class is thread-safe.
 * However, accessing global data (like data from )
 * is not thread-safe.
 *
 * Objects of this interface only live for a single superstep.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 * @param <M1> Incoming message type
 * @param <M2> Outgoing message type
 */
public interface Computation<I, V, E, M1, M2> {
  /**
   * Must be defined by user to do computation on a single Vertex.
   *
   * @param vertex   Vertex
   * @param messages Messages that were sent to this vertex in the previous
   *                 superstep.  Each message is only guaranteed to have
   *                 a life expectancy as long as next() is not called.
   */
  void compute(Vertex<I, V, E> vertex, Iterable<M1> messages)
      throws IOException;

  /**
   * Prepare for computation. This method is executed exactly once prior to
   * {@link #compute(Vertex, Iterable)} being called for any of the vertices
   * in the partition.
   */
  void preSuperstep();

  /**
   * Finish computation. This method is executed exactly once after computation
   * for all vertices in the partition is complete.
   */
  void postSuperstep();


  /**
   * Retrieves the current superstep.
   *
   * @return Current superstep
   */
  long getSuperstep();

  /**
   * Get the total (all workers) number of vertices that
   * existed in the previous superstep.
   *
   * @return Total number of vertices (-1 if first superstep)
   */
  long getTotalNumVertices();

  /**
   * Get the total (all workers) number of edges that
   * existed in the previous superstep.
   *
   * @return Total number of edges (-1 if first superstep)
   */
  long getTotalNumEdges();

  /**
   * Send a message to a vertex id.
   *
   * @param id Vertex id to send the message to
   * @param message Message data to send
   */
  void sendMessage(I id, M2 message);

  /**
   * Send a message to all edges.
   *
   * @param vertex Vertex whose edges to send the message to.
   * @param message Message sent to all edges.
   */
  void sendMessageToAllEdges(Vertex<I, V, E> vertex, M2 message);

  /**
   * Send a message to multiple target vertex ids in the iterator.
   *
   * @param vertexIdIterator An iterator to multiple target vertex ids.
   * @param message Message sent to all targets in the iterator.
   */
  void sendMessageToMultipleEdges(Iterator<I> vertexIdIterator, M2 message);

  /**
   * Sends a request to create a vertex that will be available during the
   * next superstep.
   *
   * @param id Vertex id
   * @param value Vertex value
   * @param edges Initial edges
   * @throws IOException
   */
  void addVertexRequest(I id, V value, OutEdges<I, E> edges) throws IOException;

  /**
   * Sends a request to create a vertex that will be available during the
   * next superstep.
   *
   * @param id Vertex id
   * @param value Vertex value
   * @throws IOException
   */
  void addVertexRequest(I id, V value) throws IOException;

  /**
   * Request to remove a vertex from the graph
   * (applied just prior to the next superstep).
   *
   * @param vertexId Id of the vertex to be removed.
   * @throws IOException
   */
  void removeVertexRequest(I vertexId) throws IOException;

  /**
   * Request to add an edge of a vertex in the graph
   * (processed just prior to the next superstep)
   *
   * @param sourceVertexId Source vertex id of edge
   * @param edge Edge to add
   * @throws IOException
   */
  void addEdgeRequest(I sourceVertexId, Edge<I, E> edge) throws IOException;

  /**
   * Request to remove all edges from a given source vertex to a given target
   * vertex (processed just prior to the next superstep).
   *
   * @param sourceVertexId Source vertex id
   * @param targetVertexId Target vertex id
   * @throws IOException
   */
  void removeEdgesRequest(I sourceVertexId, I targetVertexId)
      throws IOException;


}
