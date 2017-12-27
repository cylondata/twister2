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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import java.util.stream.Stream;

public class TaskParser {

  private static final Logger LOGGER = Logger.getLogger(TaskParser.class.getName());
  private static int jobId = 0;
  private DataflowTaskGraphGenerator dataflowTaskGraph;
  private TaskParser taskGraphParser;
  private Executor executor = new Executor();
  private CManager communicationManager = new CManager("msg");
  private AtomicInteger totalRunningTasks = new AtomicInteger(0);

  public TaskParser(DataflowTaskGraphGenerator taskgraph) {
    this.dataflowTaskGraph = taskgraph;
  }

  public void taskGraphParseAndSchedule() {
    Set<TaskMapper> processedTaskVertices;
    if (dataflowTaskGraph != null) {
      processedTaskVertices = dataflowTaskGraphPrioritize(this.dataflowTaskGraph);
      try {
        System.out.println("Processed Task Vertices Size Is:"
            + processedTaskVertices.size() + "\t" + processedTaskVertices);
      } catch (NullPointerException npe) {
        npe.printStackTrace();
      }
      processedTaskVertices.forEach(System.out::println);
      processedTaskVertices.stream().
          forEach(TaskMapper -> executor.execute(TaskMapper));

    }
  }

  public void taskGraphParseAndSchedule(int containerId) {
    Set<TaskMapper> processedTaskVertices;
    if (dataflowTaskGraph != null) {
      processedTaskVertices = dataflowTaskGraphPrioritize(this.dataflowTaskGraph);
      processedTaskVertices.forEach(System.out::println);
      if (!processedTaskVertices.isEmpty()) {
        if (containerId == 0) {
          executor.execute(processedTaskVertices.iterator().next());
        } else if (containerId == 1) {
          int index = 0;
          for (TaskMapper processedTask : processedTaskVertices) {
            if (index == 0) {
              ++index;
            } else if (index == 1) {
              executor.execute(processedTask);
            }
          }
        }
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
      CManager> dataflowTaskgraph, final TaskMapper mapper) {

    LOGGER.info("Dataflow Task Graph is:" + dataflowTaskgraph
        + "\t" + "and Task Object is:" + mapper);

    if (dataflowTaskgraph.outDegreeOf(mapper) == 0) {
      return 1;
    } else {
      Set<CManager> taskEdgesOf = dataflowTaskgraph.
          outgoingTaskEdgesOf(mapper);
      Stream<TaskMapper> taskStream = taskEdgesOf.stream().map(
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
