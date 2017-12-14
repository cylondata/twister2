package edu.iu.dsc.tws.task.taskgraphbuilder;

import java.util.Set;

public abstract class TaskGraphs {

  public static <TV, TE> boolean addTaskGraph(
      ITaskGraph<? super TV, ? super TE> target,
      ITaskGraph<TV, TE> source) {

    boolean value = addAllTaskVertices(target, source.getTaskVertexSet());
    value |= addAllTaskEdges(target, source, source.taskEdgeSet());

    return value;
  }

  private static <TV, TE> boolean addAllTaskEdges(
      ITaskGraph<? super TV, ? super TE> target,
      ITaskGraph<TV, TE> source,
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
      ITaskGraph<? super TV, ? super TE> target, Set<TV> taskVertices) {

    boolean flag = false;

    for (TV taskVertex : taskVertices) {
      flag |= target.addTaskVertex(taskVertex);
    }
    return flag;
  }

  public static <TV, TE> TE addTaskEdge(
      ITaskGraph<TV, TE> taskGraph,
      TV sourceTaskVertex,
      TV targetTaskVertex)
      throws InstantiationException, IllegalAccessException {

    IDataflowTaskEdgeFactory<TV, TE> taskEdgeFactory = taskGraph.getDataflowTaskEdgeFactory();
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
      ITaskGraph<TV, TE> taskGraph,
      TV sourceTaskVertex,
      TV targetTaskVertex) {

    taskGraph.addTaskVertex(sourceTaskVertex);
    taskGraph.addTaskVertex(targetTaskVertex);

    return taskGraph.addTaskEdge(sourceTaskVertex, targetTaskVertex);
  }

  public static <TV, TE> boolean addTaskEdgeWithVertices(
      ITaskGraph<TV, TE> sourceTaskGraph,
      ITaskGraph<TV, TE> targetTaskGraph,
      TE taskEdge) {

    TV sourceTaskVertex = sourceTaskGraph.getTaskEdgeSource(taskEdge);
    TV targetTaskVertex = sourceTaskGraph.getTaskEdgeTarget(taskEdge);

    targetTaskGraph.addTaskVertex(sourceTaskVertex);
    targetTaskGraph.addTaskVertex(targetTaskVertex);

    return targetTaskGraph.addTaskEdge(sourceTaskVertex, targetTaskVertex, taskEdge);
  }
}


