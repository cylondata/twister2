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
package edu.iu.dsc.tws.dl.graph;

import java.io.Serializable;

/**
 * An edge in the graph
 */
public class Edge implements Serializable {
  //fromIndex A preserved position to store meta info.
  private Integer fromIndex = null;

  public Edge() {
  }

  public Edge(Integer fromIndex) {
    this.fromIndex = fromIndex;
  }

  public Integer getFromIndex() {
    return fromIndex;
  }

  public void setFromIndex(Integer fromIndex) {
    this.fromIndex = fromIndex;
  }

  /**
   * Create a new Instance of this Edge
   *
   * @return a new Instance of this Edge
   */
  public Edge newInstance() {
    if (fromIndex == null) {
      return new Edge(fromIndex);
    } else {
      return new Edge(null);
    }
  }

  @Override
  public String toString() {
    return "Edge{" + "fromIndex=" + fromIndex + '}';
  }
}
