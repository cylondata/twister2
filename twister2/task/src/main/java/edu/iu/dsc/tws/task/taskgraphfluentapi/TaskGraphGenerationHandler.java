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

public class TaskGraphGenerationHandler implements ITaskGraphGenerate {

  @Override
  public ITaskGraphGenerate generateTaskVertex(int index) {
    return null;
  }

  @Override
  public ITaskGraphGenerate generateTaskEdge() {
    return null;
  }

  @Override
  public ITaskGraphGenerate connectTaskVertex_Edge(int taskVertexId, int... taskEdgeId) {
    return null;
  }

  @Override
  public ITaskGraphGenerate connectTaskVertex_Edge(ITaskInfo taskVertex, ITaskInfo... taskEdge) {
    return null;
  }

  @Override
  public ITaskGraphGenerate connectTaskVertex_Edge(DataflowOperation dataflowOperation,
                                                   ITaskInfo taskVertex, ITaskInfo... taskEdge) {
    return null;
  }

  @Override
  public ITaskGraphGenerate generateITaskGraph(DataflowOperation dataflowOperation,
                                               ITaskInfo taskVertex, ITaskInfo... taskEdge) {
    return null;
  }

  @Override
  public ITaskGraphGenerate submitToTaskGraphGenerator() {
    return null;
  }

  @Override
  public ITaskGraphGenerate parseTaskGraph() {
    return null;
  }

  @Override
  public ITaskGraphGenerate generateExecutionGraph(ITaskGraphGenerate taskGraph) {
    return null;
  }

  @Override
  public ITaskGraphGenerate submitToTaskExecutor() {
    return null;
  }

  @Override
  public ITaskGraphGenerate build() {
    return null;
  }

  @Override
  public void displayTaskGraph() {

  }

  public void showTaskGraph() {

  }
}
