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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import java.util.stream.Stream;

import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.task.api.Task;

public class DataflowTaskGraphParser {

  private static final Logger LOGGER = Logger.getLogger(
      DataflowTaskGraphParser.class.getName());

  private static int jobId = 0;
  private DataflowTaskGraphGenerator dataflowTaskGraph;
  private DataflowTaskGraphParser dataflowTaskGraphParser;
  private Executor executor = new Executor();
  private DataFlowOperation dataFlowOperation = null;
  private AtomicInteger totalRunningTasks = new AtomicInteger(0);

  public DataflowTaskGraphParser(DataflowTaskGraphGenerator taskgraph) {
    this.dataflowTaskGraph = taskgraph;
  }

  @SuppressWarnings("unchecked")
  public Set<Task> dataflowTaskGraphParseAndSchedule() {
    Set<Task> processedTaskVertices = new HashSet<>();

    if (dataflowTaskGraph != null) {
      processedTaskVertices = dataflowTaskGraphPrioritize(this.dataflowTaskGraph);

      LOGGER.info("Processed Task Vertices Size Is:"
          + processedTaskVertices.size() + "\t" + processedTaskVertices);
      try {
        processedTaskVertices.forEach(System.out::println);
        processedTaskVertices.stream().
            forEach(Mapper -> executor.execute(Mapper,
                (Class<DataFlowOperation>) dataFlowOperation.getClass()));
        ++jobId;
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    return processedTaskVertices;
  }

  private Set<Task> dataflowTaskGraphPrioritize(DataflowTaskGraphGenerator taskGraph) {
    final IDataflowTaskGraph<Task, DataFlowOperation>
        dataflowTaskgraph = taskGraph.getDataflowGraph();

    Set<Task> taskVertices = dataflowTaskgraph.getTaskVertexSet();
    try {
      taskVertices.stream()
          .filter(task -> dataflowTaskgraph.inDegreeOf(task) == 0)
          .forEach(task -> dataflowTaskGraphParse(dataflowTaskgraph, task));
    } catch (NullPointerException npe) {
      npe.printStackTrace();
    }
    return taskVertices;
  }

  private int dataflowTaskGraphParse(final IDataflowTaskGraph<Task,
      DataFlowOperation> dataflowTaskgraph, final Task mapper) {

    LOGGER.info("Dataflow Task Graph is:" + dataflowTaskgraph
        + "\t" + "and Task Object is:" + mapper);

    if (dataflowTaskgraph.outDegreeOf(mapper) == 0) {
      return 1;
    } else {
      Set<DataFlowOperation> taskEdgesOf = dataflowTaskgraph.
          outgoingTaskEdgesOf(mapper);
      Stream<Task> taskStream = taskEdgesOf.stream().map(
          dataflowTaskgraph::getTaskEdgeTarget);
      int maxWeightOfNeighbours = taskStream.map(
          next -> dataflowTaskGraphParse(dataflowTaskgraph, next)).
          max(Integer::compare).get();
      //For testing....
      int weightOfCurrent = 1 + maxWeightOfNeighbours;
      LOGGER.info("Task Edges are:" + taskEdgesOf + "\t"
          + "taskStream:" + taskStream.getClass().getName() + "\t"
          + "Weight of the current task:" + weightOfCurrent);
      return weightOfCurrent;
    }
  }
}

