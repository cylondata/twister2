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

import edu.iu.dsc.tws.task.api.ITask;
import edu.iu.dsc.tws.task.api.Task;

public class DataflowTGraphParser implements IDataflowTaskGraphParser {

  private static final Logger LOG = Logger.getLogger(
      DataflowTGraphParser.class.getName());

  private DataflowTaskGraphGenerator dataflowTaskGraph;

  public DataflowTGraphParser(DataflowTaskGraphGenerator taskgraph) {
    this.dataflowTaskGraph = taskgraph;
  }

  @Override
  public Set<Task> dataflowTaskGraphParseAndSchedule(int containerId) {
    return null;
  }

  @Override
  public Set<Task> dataflowTaskGraphParseAndSchedule(String message) {
    return null;
  }

  @SuppressWarnings("unchecked")
  /**
   * This is an entry method to invoke the dataflow task graph
   * prioritizer to prioritize the tasks.
   */
  public Set<TaskGraphMapper> dataflowTGraphParseAndSchedule() {

    Set<TaskGraphMapper> processedTaskVertices = new HashSet<>();
    if (dataflowTaskGraph != null) {
      processedTaskVertices = dataflowTaskGraphPrioritize(this.dataflowTaskGraph);
      LOG.info("Processed Task Vertices Size Is:"
          + processedTaskVertices.size() + "\t" + processedTaskVertices);
    }
    return processedTaskVertices;
  }

  @Override
  public Set<ITask> taskGraphParseAndSchedule() {
    return null;
  }

  /**
   * This method calls the dataflow task graph parser method to prioritize
   * the tasks which is ready for the execution.
   */
  private Set<TaskGraphMapper> dataflowTaskGraphPrioritize(DataflowTaskGraphGenerator taskGraph) {

    final IDataflowTaskGraph<TaskGraphMapper, DataflowOperation>
        dataflowTaskgraph = taskGraph.getTGraph();
    Set<TaskGraphMapper> taskVertices = dataflowTaskgraph.getTaskVertexSet();

    //Newly Added on April 5th, 2018
    Set<SourceTargetTaskDetails> sourceTargetTaskDetailsSet = new HashSet<>();
    for (TaskGraphMapper child : taskVertices) {
      sourceTargetTaskDetailsSet = dataflowTaskSourceTargetVertices(dataflowTaskgraph, child);
      if (!sourceTargetTaskDetailsSet.isEmpty()) {
        for (SourceTargetTaskDetails sourceTargetTaskDetails : sourceTargetTaskDetailsSet) {
          /*LOG.info("Source and Target Task Details:"
              + sourceTargetTaskDetails.getSourceTask() + "--->"
              + sourceTargetTaskDetails.getTargetTask() + "---"
              + "Source Task Id and Name" + "---"
              + sourceTargetTaskDetails.getSourceTask().getTaskId() + "---"
              + sourceTargetTaskDetails.getSourceTask().getTaskName() + "----"
              + "Target Task Id and Name" + "---"
              + sourceTargetTaskDetails.getTargetTask().getTaskId() + "---"
              + sourceTargetTaskDetails.getTargetTask().getTaskName() + "---"
              + sourceTargetTaskDetails.getDataflowOperationName());*/
        }
      }
    }
    try {
      taskVertices.stream()
          .filter(task -> dataflowTaskgraph.inDegreeOfTask(task) == 0)
          .forEach(task -> dataflowTaskGraphParse(dataflowTaskgraph, task));
    } catch (NullPointerException npe) {
      npe.printStackTrace();
    }
    return taskVertices;
  }

  /**
   * This method displays the task edges, its child, source and target task vertices of a particular
   * task
   */
  private Set<SourceTargetTaskDetails> dataflowTaskSourceTargetVertices(
      final IDataflowTaskGraph<TaskGraphMapper,
          DataflowOperation> dataflowTGraph,
      final TaskGraphMapper mapper) {

    LOG.info("Task Object is:" + mapper + "\t"
        + "Task Id:" + mapper.getTaskId() + "\t"
        + "Task Name:" + mapper.getTaskName());

    Set<SourceTargetTaskDetails> childTask = new HashSet<>();
    if (dataflowTGraph.outDegreeOfTask(mapper) == 0) {
      return childTask;
    } else {
      Set<DataflowOperation> taskEdgesOf = dataflowTGraph.outgoingTaskEdgesOf(mapper);
      LOG.info("Task Child Size:" + dataflowTGraph.outgoingTaskEdgesOf(mapper) + "\n");
      for (DataflowOperation edge : taskEdgesOf) {
        SourceTargetTaskDetails sourceTargetTaskDetails = new SourceTargetTaskDetails();
        //sourceTargetTaskDetails.setSourceTask(dataflowTGraph.getTaskEdgeSource(edge));
        //sourceTargetTaskDetails.setTargetTask(dataflowTGraph.getTaskEdgeTarget(edge));
        sourceTargetTaskDetails.setDataflowOperation(edge);
        sourceTargetTaskDetails.setDataflowOperationName(edge.getDataflowOperation());
        childTask.add(sourceTargetTaskDetails);

        /*LOG.info("%%%% Dataflow Operation:" + edge.getDataflowOperation());
        LOG.info("%%%% Source and Target Vertex:" + dataflowTGraph.getTaskEdgeSource(edge)
           + "\t" + dataflowTGraph.getTaskEdgeTarget(edge));*/
      }
      /*for (SourceTargetTaskDetails sourceTargetTaskDetails : childTask) {
        LOG.info("Source and Target Task Details:"
            + sourceTargetTaskDetails.getSourceTask() + "--->"
            + sourceTargetTaskDetails.getTargetTask() + "---"
            + sourceTargetTaskDetails.getSourceTask().getTaskId() + "---"
            + sourceTargetTaskDetails.getSourceTask().getTaskName() + "---->"
            + sourceTargetTaskDetails.getTargetTask().getTaskId() + "---"
            + sourceTargetTaskDetails.getTargetTask().getTaskName() + "---"
            + sourceTargetTaskDetails.getDataflowOperationName());
      }*/
      return childTask;
    }
  }

  /**
   * This is the simple dataflow task graph parser method and it should be replaced
   * with an optimized scheduling mechanism.
   */
  private int dataflowTaskGraphParse(final IDataflowTaskGraph<TaskGraphMapper,
      DataflowOperation> dataflowTaskgraph,
                                     final TaskGraphMapper mapper) {
    if (dataflowTaskgraph.outDegreeOfTask(mapper) == 0) {
      return 1;
    } else {
      Set<DataflowOperation> taskEdgesOf = dataflowTaskgraph.
          outgoingTaskEdgesOf(mapper);

      //This is perfectly working code to find out the child using the task edge.
      /*LOG.info("Task Edges Size:" + taskEdgesOf.size() + "\t" + taskEdgesOf.iterator().next());
      LOG.info("Task Child:" + dataflowTaskgraph.outgoingTaskEdgesOf(mapper) + "\n");
      for (DataflowOperation edge : taskEdgesOf) {
        LOG.info("Dataflow Operation:" + edge.getDataflowOperation());
        LOG.info("Source and Target Vertex:" + dataflowTaskgraph.getTaskEdgeSource(edge)
            + "\t" + dataflowTaskgraph.getTaskEdgeTarget(edge));
      }*/

      Stream<TaskGraphMapper> taskStream = taskEdgesOf.stream().
          map(dataflowTaskgraph::getTaskEdgeTarget);

      int adjacentTaskWeights = taskStream.map(
          next -> dataflowTaskGraphParse(dataflowTaskgraph, next)).
          max(Integer::compare).get();
      int weightOfCurrent = 1 + adjacentTaskWeights;

      /*LOG.info("Task Edges are:" + taskEdgesOf.iterator().next().getDataflowOperation() + "\t"
          + "neighbours:" + taskStream.getClass().getName() + "\t"
          + "Weight of the current task:" + weightOfCurrent);*/

      return weightOfCurrent;
    }
  }

  @Override
  public Set<ITask> taskGraphParseAndSchedule(int containerId) {
    return null;
  }

}


