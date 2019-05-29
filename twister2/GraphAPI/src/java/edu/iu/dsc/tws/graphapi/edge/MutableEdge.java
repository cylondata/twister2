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
 * An edge whose value can be modified.
 *
 * @param <I> Vertex index
 * @param <E> Edge value
 */
public interface MutableEdge<I, E>
    extends Edge<I, E> {
  /**
   * Set the value for this edge.
   *
   * @param value new edge value
   */
  void setValue(E value);
}
