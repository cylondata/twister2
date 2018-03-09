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
package edu.iu.dsc.tws.task.taskgraphfluentapi;

import edu.iu.dsc.tws.task.taskgraphbuilder.DataflowOperation;

/**
 * This interface will be used by the user to connect the
 * generated task objects (vertexes) with task edges. And, submit the
 * task objects to generate the task graph.
 */
public interface ITaskGraphGenerate {

  ITaskGraphGenerate generateTaskVertex(int index);

  //ITaskGraphGenerate generateTaskEdge();

  ITaskGraphGenerate connectTaskVertex_Edge(int taskVertexId, int... taskEdgeId);

  ITaskGraphGenerate connectTaskVertex_Edge(ITaskInfo taskVertex, ITaskInfo... taskEdge);

  ITaskGraphGenerate connectTaskVertex_Edge(DataflowOperation dataflowOperation,
                                            ITaskInfo taskVertex, ITaskInfo... taskEdge);

  ITaskGraphGenerate generateITaskGraph(DataflowOperation dataflowOperation,
                                        ITaskInfo taskVertex, ITaskInfo... taskEdge);

  ITaskGraphGenerate build();

  void displayTaskGraph();

  ITaskGraphGenerate submit();

  ITaskGraphGenerate submitToTaskGraphGenerator();

  ITaskGraphGenerate parseTaskGraph();

  ITaskGraphGenerate generateExecutionGraph(ITaskGraphGenerate taskGraph);

  ITaskGraphGenerate submitToTaskExecutor();
}

