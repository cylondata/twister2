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
package edu.iu.dsc.tws.comms.op;

import java.util.HashSet;
import java.util.Set;

public class EdgeGenerator {
  private int current;

  public EdgeGenerator(int curr) {
    this.current = curr;
  }

  public int nextEdge() {
    this.current++;
    return current;
  }

  public Set<Integer> nextEdge(int number) {
    Set<Integer> edges = new HashSet<>();
    for (int i = 0; i < number; i++) {
      current++;
      edges.add(current);
    }
    return edges;
  }
}
