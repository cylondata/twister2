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
package edu.iu.dsc.tws.task.taskgraphbuilder;

import java.util.Set;

public abstract class DirectedDataflowTaskGraphAbstraction<TV, TE> {

  public abstract void addTaskVertex(TV taskVertex);

  public abstract void addTaskEdgeTVertices(TE taskEdge);

  public abstract void removeTaskEdgeTVertices(TE taskEdge);

  public abstract Set<TV> getTaskVertexSet();

  public abstract TE getTaskEdge(TV sourceTaskVertex, TV targetTaskVertex);

  public abstract Set<TE> getAllTaskEdges(TV sourceTaskVertex,
                                          TV targetTaskVertex);

  public abstract Set<TE> taskEdgesOf(TV taskVertex);

  public abstract Set<TE> incomingTaskEdgesOf(TV taskVertex);

  public abstract Set<TE> outgoingTaskEdgesOf(TV taskVertex);

  public abstract int inDegreeOf(TV taskVertex);

  public abstract int outDegreeOf(TV taskVertex);
}
