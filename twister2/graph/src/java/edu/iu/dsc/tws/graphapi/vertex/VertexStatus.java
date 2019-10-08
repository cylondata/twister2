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

public interface VertexStatus<I, V> {
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

  void setId(I id);

  /**
   * Set the vertex data (immediately visible in the computation)
   *
   * @param value Vertex data to be set
   */
  void setValue(V value);
}
