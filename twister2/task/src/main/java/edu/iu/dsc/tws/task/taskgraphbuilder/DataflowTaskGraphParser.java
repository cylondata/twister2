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

import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Stream;

public class DataflowTaskGraphParser {

  private static final Logger LOGGER = Logger.getLogger(DataflowTaskGraphParser.class.getName());
  private static int jobId = 0;
  private DataflowTaskGraphGenerator dataflowTaskGraph;
  private DataflowTaskGraphParser taskGraphParser;
  private Executor executor = new Executor();
  private CManager cManager = new CManager("msg");

  public DataflowTaskGraphParser(DataflowTaskGraphGenerator taskgraph) {
    this.dataflowTaskGraph = taskgraph;
  }

  @SuppressWarnings("unchecked")
  public void dataflowTaskGraphParseAndSchedule() {
    Set<TaskMapper> processedTaskVertices = new HashSet<>();

    if (dataflowTaskGraph != null) {
      processedTaskVertices = dataflowTaskGraphPrioritize(this.dataflowTaskGraph);

      LOGGER.info("Processed Task Vertices Size Is:"
          + processedTaskVertices.size() + "\t" + processedTaskVertices);
      try {
        processedTaskVertices.forEach(System.out::println);
        processedTaskVertices.stream().
            forEach(Mapper -> executor.execute(Mapper,
                (Class<CManager>) cManager.getClass()));
        ++jobId;
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
  }

  public Set<TaskMapper> dataflowTaskGraphPrioritize(DataflowTaskGraphGenerator taskGraph) {
    final IDataflowTaskGraph<TaskMapper, CManager> dataflowGraph =
        taskGraph.getDataflowTaskGraph();
    Set<TaskMapper> taskVertices = dataflowGraph.getTaskVertexSet();

    try {
      taskVertices.stream()
          .filter(task -> dataflowGraph.inDegreeOf(task) == 0)
          .forEach(task -> dataflowTaskGraphParse(dataflowGraph, task));
    } catch (NullPointerException npe) {
      npe.printStackTrace();
    }
    return taskVertices;
  }

  private int dataflowTaskGraphParse(final IDataflowTaskGraph<TaskMapper,
      CManager> dataflowGraph, final TaskMapper mapper) {

    LOGGER.info("Dataflow Task Graph is:" + dataflowGraph
        + "\t" + "and Task Object is:" + mapper);

    if (mapper.hasExecutionWeight()) {
      return (int) mapper.getExecutionWeight();
    }
    if (dataflowGraph.outDegreeOf(mapper) == 0) {
      mapper.setExecutionWeight(mapper.getTaskPriority());
      return (int) mapper.getExecutionWeight();
    } else {
      Set<CManager> taskEdges = dataflowGraph.outgoingTaskEdgesOf(mapper);
      Stream<TaskMapper> neighbours = taskEdges.stream().map(dataflowGraph::getTaskEdgeTarget);

      int maxWeightOfNeighbours = neighbours.map(
          next -> dataflowTaskGraphParse(dataflowGraph, next)).max(Integer::compare).get();
      int weightOfCurrent = mapper.getTaskPriority() + maxWeightOfNeighbours;
      mapper.setExecutionWeight(weightOfCurrent);

      LOGGER.info("Task Edges are:" + taskEdges + "\t"
          + "neighbours:" + neighbours.getClass().getName() + "\t"
          + "Weight of the current task:" + weightOfCurrent);

      return weightOfCurrent;
    }
  }
}
