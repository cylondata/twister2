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

import java.util.HashSet;
import java.util.Set;
import java.util.logging.Logger;
import java.util.stream.Stream;

import edu.iu.dsc.tws.task.api.ITask;

public class DataflowTaskGraphParser {

  private static final Logger LOG = Logger.getLogger(
      DataflowTaskGraphParser.class.getName());

  private DataflowTaskGraphGenerator dataflowTaskGraph;

  public DataflowTaskGraphParser(DataflowTaskGraphGenerator taskgraph) {
    this.dataflowTaskGraph = taskgraph;
  }

  @SuppressWarnings("unchecked")
  public Set<ITask> taskGraphParseAndSchedule() {
    Set<ITask> processedTaskVertices = new HashSet<>();
    if (dataflowTaskGraph != null) {
      processedTaskVertices = dataflowTaskGraphPrioritize(this.dataflowTaskGraph);
      LOG.info("Processed Task Vertices Size Is:"
          + processedTaskVertices.size() + "\t" + processedTaskVertices);
    }
    return processedTaskVertices;
  }

  /**
   * This method calls the dataflow task graph parser method to prioritize
   * the tasks which is ready for the execution.
   */
  private Set<ITask> dataflowTaskGraphPrioritize(DataflowTaskGraphGenerator taskGraph) {

    final ITaskGraph<ITask, TaskEdge> dataflowTaskgraph = taskGraph.getTaskgraph();
    Set<ITask> taskVertices = dataflowTaskgraph.getTaskVertexSet();

    //Newly Added on April 5th, 2018
    /*Set<SourceTargetTaskDetails> sourceTargetTaskDetailsSet = new HashSet<>();
    for (ITask child : taskVertices) {
      sourceTargetTaskDetailsSet = dataflowTaskSourceTargetVertices(dataflowTaskgraph, child);
      if (!sourceTargetTaskDetailsSet.isEmpty()) {
        for (SourceTargetTaskDetails sourceTargetTaskDetails : sourceTargetTaskDetailsSet) {
          LOG.info("Source and Target Task Details:"
              + sourceTargetTaskDetails.getSourceTask() + "--->"
              + sourceTargetTaskDetails.getTargetTask() + "---"
              + "Source Task Id and Name" + "---"
              + sourceTargetTaskDetails.getSourceTask().taskName() + "----"
              + "Target Task Id and Name" + "---"
              + sourceTargetTaskDetails.getTargetTask().taskName() + "---"
              + sourceTargetTaskDetails.getDataflowOperationName() + "\n");
        }
      }
    }*/

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
      final ITaskGraph<ITask,
          TaskEdge> dataflowtaskgraph,
      final ITask mapper) {

    LOG.info("Task Object is:" + mapper + "\t"
        // + "Task Id:" + mapper.getTaskId() + "\t"
        + "Task Name:" + mapper.taskName());

    Set<SourceTargetTaskDetails> childTask = new HashSet<>();
    if (dataflowtaskgraph.outDegreeOfTask(mapper) == 0) {
      return childTask;
    } else {
      Set<TaskEdge> taskEdgesOf = dataflowtaskgraph.outgoingTaskEdgesOf(mapper);
      LOG.info("Task Child Size:" + dataflowtaskgraph.outgoingTaskEdgesOf(mapper) + "\n");
      for (TaskEdge edge : taskEdgesOf) {
        SourceTargetTaskDetails sourceTargetTaskDetails = new SourceTargetTaskDetails();
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
  private int dataflowTaskGraphParse(final ITaskGraph<ITask,
      TaskEdge> dataflowTaskgraph,
                                     final ITask mapper) {
    if (dataflowTaskgraph.outDegreeOfTask(mapper) == 0) {
      return 1;
    } else {
      Set<TaskEdge> taskEdgesOf = dataflowTaskgraph.
          outgoingTaskEdgesOf(mapper);

      Stream<ITask> taskStream = null;

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
}

