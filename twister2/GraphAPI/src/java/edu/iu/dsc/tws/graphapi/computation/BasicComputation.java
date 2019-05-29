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
package edu.iu.dsc.tws.graphapi.computation;

import java.io.IOException;
import java.util.Iterator;

import edu.iu.dsc.tws.graphapi.edge.Edge;
import edu.iu.dsc.tws.graphapi.edge.OutEdges;
import edu.iu.dsc.tws.graphapi.vertex.Vertex;

/**
 * Computation in which both incoming and outgoing message types are the same.
 *
 * @param <I> Vertex id
 * @param <V> Vertex data
 * @param <E> Edge data
 * @param <M> Message type
 */
public  class BasicComputation<I, V, E, M>  implements Computation<I, V, E, M, M> {
  @Override
  public void compute(Vertex<I, V, E> vertex, Iterable<M> messages) throws IOException {

  }

  @Override
  public void preSuperstep() {

  }

  @Override
  public void postSuperstep() {

  }

  @Override
  public long getSuperstep() {
    return 0;
  }

  @Override
  public long getTotalNumVertices() {
    return 0;
  }

  @Override
  public long getTotalNumEdges() {
    return 0;
  }

  @Override
  public void sendMessage(I id, M message) {

  }

  @Override
  public void sendMessageToAllEdges(Vertex<I, V, E> vertex, M message) {

  }

  @Override
  public void sendMessageToMultipleEdges(Iterator<I> vertexIdIterator, M message) {

  }

  @Override
  public void addVertexRequest(I id, V value, OutEdges<I, E> edges) throws IOException {

  }

  @Override
  public void addVertexRequest(I id, V value) throws IOException {

  }

  @Override
  public void removeVertexRequest(I vertexId) throws IOException {

  }

  @Override
  public void addEdgeRequest(I sourceVertexId, Edge<I, E> edge) throws IOException {

  }

  @Override
  public void removeEdgesRequest(I sourceVertexId, I targetVertexId) throws IOException {

  }
}
