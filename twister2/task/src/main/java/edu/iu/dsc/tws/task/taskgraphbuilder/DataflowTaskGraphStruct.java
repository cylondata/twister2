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

public abstract class DataflowTaskGraphStruct {

  /**
   * This method serves as a reference to the method in Dataflow Task Graph Abstraction class.
   */
  public static <TV, TE> boolean addTaskGraph(
      ITaskGraph<? super TV, ? super TE> target,
      ITaskGraph<TV, TE> source) {

    boolean value = addAllTaskVertices(target, source.getTaskVertexSet());
    value |= addAllTaskEdges(target, source, source.taskEdgeSet());
    return value;
  }

  /**
   * This method is responsible for adding all the task edges
   * to the dataflow task graph.
   */
  private static <TV, TE> boolean addAllTaskEdges(
      ITaskGraph<? super TV, ? super TE> target,
      ITaskGraph<TV, TE> source,
      Set<TE> taskEdges) {

    boolean value = false;
    for (TE taskEdge : taskEdges) {
      TV sourceTask = source.getTaskEdgeSource(taskEdge);
      TV targetTask = source.getTaskEdgeTarget(taskEdge);

      target.addTaskVertex(sourceTask);
      target.addTaskVertex(targetTask);

      value |= target.addTaskEdge(sourceTask, targetTask, taskEdge);
    }
    return value;
  }

  /**
   * This method is responsible for adding all the dataflow task
   * vertices to the graph.
   */
  private static <TV, TE> boolean addAllTaskVertices(
      ITaskGraph<? super TV, ? super TE> target, Set<TV> taskVertices) {

    boolean value = false;
    for (TV taskVertex : taskVertices) {
      value |= target.addTaskVertex(taskVertex);
    }
    return value;
  }

  public static <TV, TE> TE addTaskEdge(
      ITaskGraph<TV, TE> taskGraph,
      TV sourceTaskVertex,
      TV targetTaskVertex)
      throws InstantiationException, IllegalAccessException {

    IDataflowTaskEdgeFactory<TV, TE> dataflowTaskEdgeFactory =
        taskGraph.getDataflowTaskEdgeFactory();
    TE taskEdge = null;
    try {
      taskEdge = dataflowTaskEdgeFactory.createTaskEdge(sourceTaskVertex, targetTaskVertex);
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    } catch (InstantiationException e) {
      e.printStackTrace();
    }
    return taskGraph.addTaskEdge(
        sourceTaskVertex, targetTaskVertex, taskEdge) ? taskEdge : null;
  }

  public static <TV, TE> TE addTaskEdgeWithVertices(
      ITaskGraph<TV, TE> taskGraph,
      TV sourceTaskVertex,
      TV targetTaskVertex) {

    taskGraph.addTaskVertex(sourceTaskVertex);
    taskGraph.addTaskVertex(targetTaskVertex);

    return taskGraph.addTaskEdge(sourceTaskVertex, targetTaskVertex);
  }
}

