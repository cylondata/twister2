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

public abstract class TaskGraphs {

  public static <TV, TE> boolean addTaskGraph(
      TaskGraph<? super TV, ? super TE> target,
      TaskGraph<TV, TE> source) {

    boolean value = addAllTaskVertices(target, source.getTaskVertexSet());
    value |= addAllTaskEdges(target, source, source.taskEdgeSet());

    return value;
  }

  private static <TV, TE> boolean addAllTaskEdges(
      TaskGraph<? super TV, ? super TE> target,
      TaskGraph<TV, TE> source,
      Set<TE> taskEdges) {

    boolean flag = false;

    for (TE taskEdge : taskEdges) {
      TV sourceTask = source.getTaskEdgeSource(taskEdge);
      TV targetTask = source.getTaskEdgeTarget(taskEdge);

      target.addTaskVertex(sourceTask);
      target.addTaskVertex(targetTask);

      flag |= target.addTaskEdge(sourceTask, targetTask, taskEdge);
    }
    return flag;
  }

  private static <TV, TE> boolean addAllTaskVertices(
      TaskGraph<? super TV, ? super TE> target, Set<TV> taskVertices) {

    boolean flag = false;

    for (TV taskVertex : taskVertices) {
      flag |= target.addTaskVertex(taskVertex);
    }
    return flag;
  }

  public static <TV, TE> TE addTaskEdge(
      TaskGraph<TV, TE> taskGraph,
      TV sourceTaskVertex,
      TV targetTaskVertex)
      throws InstantiationException, IllegalAccessException {

    TaskEdgeFactory<TV, TE> taskEdgeFactory = taskGraph.getTaskEdgeFactory();
    TE taskEdge = null;
    try {
      taskEdge = taskEdgeFactory.createTaskEdge(sourceTaskVertex, targetTaskVertex);
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    } catch (InstantiationException e) {
      e.printStackTrace();
    }
    return taskGraph.addTaskEdge(
        sourceTaskVertex, targetTaskVertex, taskEdge) ? taskEdge : null;
  }

  public static <TV, TE> TE addTaskEdgeWithVertices(
      TaskGraph<TV, TE> taskGraph,
      TV sourceTaskVertex,
      TV targetTaskVertex) {

    taskGraph.addTaskVertex(sourceTaskVertex);
    taskGraph.addTaskVertex(targetTaskVertex);

    return taskGraph.addTaskEdge(sourceTaskVertex, targetTaskVertex);
  }

  public static <TV, TE> boolean addTaskEdgeWithVertices(
      TaskGraph<TV, TE> sourceTaskGraph,
      TaskGraph<TV, TE> targetTaskGraph,
      TE taskEdge) {

    TV sourceTaskVertex = sourceTaskGraph.getTaskEdgeSource(taskEdge);
    TV targetTaskVertex = sourceTaskGraph.getTaskEdgeTarget(taskEdge);

    targetTaskGraph.addTaskVertex(sourceTaskVertex);
    targetTaskGraph.addTaskVertex(targetTaskVertex);

    return targetTaskGraph.addTaskEdge(sourceTaskVertex, targetTaskVertex, taskEdge);
  }
}


