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
package edu.iu.dsc.tws.task.graph.htg;

import java.util.Collection;
import java.util.Set;

/**
 * This is the main interface for the task graph structure. A task graph 'TG' consists of set of
 * Task Vertices 'TV' and Task Edges (TE) which is mathematically denoted as Task Graph (TG) -> (TV, TE).
 */
public interface IHierarchicalTaskGraph<TG, TE> {

  /**
   * This method returns the set of task vertexes.
   */
  Set<TG> getTaskGraphSet();

  /**
   * This method returns the set of task edges.
   */
  Set<TE> taskGraphEdgeSet();

  /**
   * This method receives the source and target task vertex and return the set of task edges
   * persists in the graph.
   */
  Set<TE> getAllTaskGraphEdges(TG sourceTaskGraph, TG targetTaskGraph);

  /**
   * This method receives the source and target task vertex and create the task edge between the
   * source and target task vertex.
   * <p>
   * SourceTaskGraph --->TaskEdge---->TargetTaskGraph
   */
  TE addTaskGraphEdge(TG sourceTaskGraph, TG targetTaskGraph);

  /**
   * This method remove the particular task edge between the source task vertex and target task vertex.
   */

  TE removeTaskGraphEdge(TG sourceTaskGraph, TG targetTaskGraph);

  /**
   * This method remove all the task edges between the source task vertex and target task vertex.
   */
  Set<TE> removeAllTaskGraphEdges(TG sourceTaskGraph, TG targetTaskGraph);

  /**
   * This method returns true if the task edge is created between the source task vertex and target task vertex.
   */
  boolean addTaskGraphEdge(TG sourceTaskGraph, TG targetTaskGraph, TE taskGraphEdge);

  /**
   * This method returns true if the task vertex is created successfully in the graph.
   */
  boolean addTaskGraph(TG taskGraph);

  /**
   * This method removes all the task edges between the task vertexes.
   *
   * @return true or false
   */

  boolean removeAllTaskGraphEdges(Collection<? extends TE> taskGraphEdges);

  /**
   * This method returns true if the task edge persists in the graph between the source task vertex TV and target task
   * vertex TV.
   */
  boolean containsTaskGraphEdge(TG sourceTaskGraph, TG targetTaskGraph);

  /**
   * This method returns true if the task edge persists in the graph.
   */
  boolean containsTaskGraphEdge(TE taskGraphEdge);

  /**
   * This method returns true if the task vertex exists in the graph.
   */
  boolean containsTaskGraph(TG taskGraph);

  boolean removeTaskGraph(TG taskGraph);

  boolean removeTaskGraphEdge(TE taskGraphEdge);

  /**
   * This method removes all the task vertices between the task edges.
   */
  boolean removeAllTaskGraphs(Collection<? extends TG> taskVertices);

  Set<TE> taskGraphEdgesOf(TG taskGraph);


  /**
   * This method is responsible for returning the number of inward directed edges for the task vertex 'TV'
   */
  int inDegreeOfTaskGraph(TG taskGraph);

  /**
   * This method returns the set of incoming task edges for the task vertex 'TV'
   */
  Set<TE> incomingTaskGraphEdgesOf(TG taskGraph);

  /**
   * This method returns the set of outward task edges for the task vertex 'TV'
   */
  int outDegreeOfTaskGraph(TG taskGraph);

  /**
   * This method returns the set of outgoing task edges for the task vertex 'TV'
   */
  Set<TE> outgoingTaskGraphEdgesOf(TG taskGraph);
}


