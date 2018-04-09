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

import java.util.Collection;
import java.util.Set;

/**
 * This is the main interface for the task graph structure. A task graph 'TG' consists of set of
 * Task Vertices 'TV' and Task Edges (TE) which is mathematically denoted as Task Graph (TG) -> (TV, TE).
 */
public interface ITaskGraph<TV, TE> {

  /**
   * This method returns the set of task vertexes.
   */
  Set<TV> getTaskVertexSet();

  /**
   * This method returns the set of task edges.
   */
  Set<TE> taskEdgeSet();

  /**
   * This method receives the source and target task vertex and return the set of task edges
   * persists in the graph.
   */
  Set<TE> getAllTaskEdges(TV sourceTaskVertex, TV targetTaskVertex);

  /**
   * This method receives the source and target task vertex and create the task edge between the
   * source and target task vertex.
   * <p>
   * SourceTaskVertex --->TaskEdge---->TargetTaskVertex
   */
  TE addTaskEdge(TV sourceTaskVertex, TV targetTaskVertex);

  /**
   * This method remove the particular task edge between the source task vertex and target task vertex.
   */

  TE removeTaskEdge(TV sourceTaskVertex, TV targetTaskVertex);

  /**
   * This method remove all the task edges between the source task vertex and target task vertex.
   */
  Set<TE> removeAllTaskEdges(TV sourceTaskVertex, TV targetTaskVertex);

  /**
   * This method returns true if the task edge is created between the source task vertex and target task vertex.
   */
  boolean addTaskEdge(TV sourceTaskVertex, TV targetTaskVertex, TE taskEdge);

  /**
   * This method returns true if the task vertex is created successfully in the graph.
   */
  boolean addTaskVertex(TV taskVertex);

  /**
   * This method removes all the task edges between the task vertexes.
   *
   * @return true or false
   */

  boolean removeAllTaskEdges(Collection<? extends TE> taskEdges);

  /**
   * This method returns true if the task edge persists in the graph between the source task vertex TV and target task
   * vertex TV.
   */
  boolean containsTaskEdge(TV sourceTaskVertex, TV targetTaskVertex);

  /**
   * This method returns true if the task edge persists in the graph.
   */
  boolean containsTaskEdge(TE taskEdge);

  /**
   * This method returns true if the task vertex exists in the graph.
   */
  boolean containsTaskVertex(TV taskVertex);

  boolean removeTaskVertex(TV taskVertex);

  boolean removeTaskEdge(TE taskEdge);

  /**
   * This method removes all the task vertices between the task edges.
   */
  boolean removeAllTaskVertices(Collection<? extends TV> taskVertices);

  Set<TE> taskEdgesOf(TV taskVertex);


  /**
   * This method is responsible for returning the number of inward directed edges for the task vertex 'TV'
   */
  int inDegreeOfTask(TV taskVertex);

  /**
   * This method returns the set of incoming task edges for the task vertex 'TV'
   */
  Set<TE> incomingTaskEdgesOf(TV taskVertex);

  /**
   * This method returns the set of outward task edges for the task vertex 'TV'
   */
  int outDegreeOfTask(TV taskVertex);

  /**
   * This method returns the set of outgoing task edges for the task vertex 'TV'
   */
  Set<TE> outgoingTaskEdgesOf(TV taskVertex);
}


