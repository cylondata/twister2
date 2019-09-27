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

import edu.iu.dsc.tws.graphapi.edge.Edge;

public class DefaultEdge<I, V> implements Edge<String, Integer> {

  private String id;
  private int value;


  @Override
  public String getTargetVertexId() {
    return id;
  }

  @Override
  public Integer getValue() {
    return value;
  }

  @Override
  public void setId(String id) {
    this.id = id;
  }

  @Override
  public void setValue(Integer value) {
    this.value = value;
  }
}
