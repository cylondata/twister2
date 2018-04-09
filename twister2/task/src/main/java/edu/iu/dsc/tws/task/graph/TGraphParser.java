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

import java.util.Set;
import java.util.stream.Stream;

import edu.iu.dsc.tws.task.taskgraphfluentapi.ITaskInfo;

public class TGraphParser {

  private DataflowTaskGraphGenerator dataflowTaskGraph;
  private TaskExecutor executor = new TaskExecutor();

  private ITaskGraph<ITaskInfo, TaskEdge> taskGraph;

  public TGraphParser(ITaskGraph<ITaskInfo, TaskEdge> iTaskGraph) {
    this.taskGraph = iTaskGraph;
  }

  public TGraphParser(DataflowTaskGraphGenerator taskgraph) {
    this.dataflowTaskGraph = taskgraph;
  }

  @SuppressWarnings("unchecked")
  public void dataflowTaskGraphParseAndSchedule() {
    Set<ITaskInfo> processedTaskVertices;
    if (dataflowTaskGraph != null) {
      processedTaskVertices = dataflowTaskGraphPrioritize(this.dataflowTaskGraph);
      System.out.println("\n Processed Task Vertices Size:" + processedTaskVertices.size());
      processedTaskVertices.forEach(System.out::println);
    }
  }

  public Set<ITaskInfo> dataflowTaskGraphPrioritize(DataflowTaskGraphGenerator taskgraph) {
    final ITaskGraph<ITaskInfo, TaskEdge> dataflowTaskgraph =
        taskgraph.getITaskGraph();
    Set<ITaskInfo> taskVertices = dataflowTaskgraph.getTaskVertexSet();
    try {
      taskVertices.stream()
          .filter(task -> dataflowTaskgraph.inDegreeOfTask(task) == 0)
          .forEach(task -> dataflowTaskGraphParse(dataflowTaskgraph, task));
    } catch (NullPointerException npe) {
      npe.printStackTrace();
    }
    return taskVertices;
  }

  private int dataflowTaskGraphParse(final ITaskGraph<ITaskInfo,
      TaskEdge> dataflowTGraph, final ITaskInfo mapper) {

    System.out.println("Dataflow Task Graph is:" + dataflowTGraph
        + "\t" + "and Task Object is:" + mapper);

    if (dataflowTGraph.outDegreeOfTask(mapper) == 0) {
      return 1;
    } else {
      Set<TaskEdge> edges = dataflowTGraph.outgoingTaskEdgesOf(mapper);
      Stream<ITaskInfo> neighbours = null;
      int maxWeightOfNeighbours = neighbours.map(
          next -> dataflowTaskGraphParse(dataflowTGraph, next)).
          max(Integer::compare).get();
      int weightOfCurrent = maxWeightOfNeighbours;
      return weightOfCurrent;
    }
  }
}
