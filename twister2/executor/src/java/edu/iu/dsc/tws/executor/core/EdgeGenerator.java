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
package edu.iu.dsc.tws.executor.core;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Unique edge ids needs to be generated in order to have communication operations.
 * We will keep track of the edges used in the framework and
 */
public class EdgeGenerator {
  private int currentEdge = 0;

  private Map<String, Integer> edges = new HashMap<>();

  private Map<Integer, String> invertedEdges = new HashMap<>();

  public synchronized int generate(String edge) {
    currentEdge++;
    edges.put(edge, currentEdge);
    invertedEdges.put(currentEdge, edge);
    return currentEdge;
  }

  public synchronized Set<Integer> generate(int num) {
    Set<Integer> gen = new HashSet<>();
    for (int i = 0; i < num; i++) {
      gen.add(++currentEdge);
    }
    return gen;
  }

  public int getIntegerMapping(String edge) {
    return edges.get(edge);
  }

  public String getStringMapping(int edge) {
    return invertedEdges.get(edge);
  }
}
