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
package edu.iu.dsc.tws.task.graph;

import java.util.Comparator;

/**
 * This class is responsible for assigning the directed task edge between the task vertices.
 * @param <TV>
 * @param <TE>
 */

public class DirectedEdge<TV, TE> {
  protected TV sourceTaskVertex;
  protected TV targetTaskVertex;
  protected TE taskEdge;

  protected Comparator<TV> vertexComparator;

  public DirectedEdge() {
  }

  public DirectedEdge(Comparator<TV> vertexComparator) {
    this.vertexComparator = vertexComparator;
  }

  public DirectedEdge(TV sourceTaskVertex, TV targetTaskVertex,
                      TE taskEdge, Comparator<TV> vertexComparator) {
    this.sourceTaskVertex = sourceTaskVertex;
    this.targetTaskVertex = targetTaskVertex;
    this.taskEdge = taskEdge;
    this.vertexComparator = vertexComparator;
  }
}

