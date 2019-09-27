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
package edu.iu.dsc.tws.graphapi.edge;


/**
 * A complete edge, the target vertex and the edge value.  Can only be one
 * edge with a destination vertex id per edge map.
 *
 * @param <I> Vertex index
 * @param <E> Edge value
 */
public interface Edge<I, E> {
  /**
   * Get the target vertex index of this edge
   *
   * @return Target vertex index of this edge
   */
  I getTargetVertexId();

  /**
   * Get the edge value of the edge
   *
   * @return Edge value of this edge
   */
  E getValue();

  /**
   * Set the vertex data (immediately visible in the computation)
   *
   * @param id edge data to be set
   */
  void setId(I id);

  /**
   * Set the vertex data (immediately visible in the computation)
   *
   * @param value edge data to be set
   */
  void setValue(E value);
}
