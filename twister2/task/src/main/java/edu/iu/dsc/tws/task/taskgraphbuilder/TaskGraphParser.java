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

import edu.iu.dsc.tws.task.api.Task;

public class TaskGraphParser implements IDataflowTaskGraphParser {

  private static final Logger LOGGER = Logger.getLogger(
      TaskGraphParser.class.getName());

  private DataflowTaskGraphGenerator dataflowTaskGraph;

  public TaskGraphParser(DataflowTaskGraphGenerator taskgraph) {
    this.dataflowTaskGraph = taskgraph;
  }

  @SuppressWarnings("unchecked")
  /**
   * This is an entry method to invoke the dataflow task graph
   * prioritizer to prioritize the tasks.
   */
  public Set<Task> taskGraphParseAndSchedule() {

    Set<Task> processedTaskVertices = new HashSet<>();
    if (dataflowTaskGraph != null) {
      processedTaskVertices = dataflowTaskGraphPrioritize(this.dataflowTaskGraph);
      LOGGER.info("Processed Task Vertices Size Is:"
          + processedTaskVertices.size() + "\t" + processedTaskVertices);
    }
    return processedTaskVertices;
  }

  /**
   * This method calls the dataflow task graph parser method to prioritize
   * the tasks which is ready for the execution.
   */
  private Set<Task> dataflowTaskGraphPrioritize(DataflowTaskGraphGenerator taskGraph) {
    final IDataflowTaskGraph<Task, DataflowOperation>
        dataflowTaskgraph = taskGraph.getTaskgraph();
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

  /**
   * This is the simple dataflow task graph parser method and it should be replaced
   * with an optimized scheduling mechanism.
   */
  private int dataflowTaskGraphParse(final IDataflowTaskGraph<Task,
      DataflowOperation> dataflowTaskgraph, final Task mapper) {

    LOGGER.info("Dataflow Task Graph is:" + dataflowTaskgraph
        + "\t" + "and Task Object is:" + mapper);

    if (dataflowTaskgraph.outDegreeOf(mapper) == 0) {
      return 1;
    } else {
      Set<DataflowOperation> taskEdgesOf = dataflowTaskgraph.
          outgoingTaskEdgesOf(mapper);

      Stream<Task> taskStream = taskEdgesOf.stream().map(
          dataflowTaskgraph::getTaskEdgeTarget);

      int adjacentTaskWeights = taskStream.map(
          next -> dataflowTaskGraphParse(dataflowTaskgraph, next)).
          max(Integer::compare).get();
      int weightOfCurrent = 1 + adjacentTaskWeights;
      return weightOfCurrent;
    }
  }

  //These two methods will be implemented in the future.
  @Override
  public Set<Task> dataflowTaskGraphParseAndSchedule() {
    return null;
  }

  @Override
  public Set<Task> dataflowTaskGraphParseAndSchedule(int containerId) {
    return null;
  }

  @Override
  public Set<TaskGraphMapper> dataflowTGraphParseAndSchedule() {
    return null;
  }
}

