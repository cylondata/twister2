package edu.iu.dsc.tws.task.taskgraphbuilder;

import java.util.Collection;
import java.util.Set;

public abstract class AbstractDataflowTaskGraph<TV, TE> implements ITaskGraph<TV, TE> {
  public AbstractDataflowTaskGraph() {
  }

  @Override
  public boolean containsTaskEdge(TV sourceTaskVertex, TV targetTaskVertex) {
    boolean flag = false;
    if (!(getTaskEdge(sourceTaskVertex, targetTaskVertex) == null)) {
      flag = true;
    }
    return flag;
  }

  @Override
  public boolean removeAllTaskEdges(Collection<? extends TE> taskEdges) {
    boolean success = false;
    for (TE taskEdge : taskEdges) {
      success |= removeTaskEdge(taskEdge);
    }
    return success;
  }

  protected boolean removeAllTaskEdges(TE[] taskEdges) {
    boolean success = false;
    for (int i = 0; i < taskEdges.length; i++) {
      success |= removeTaskEdge(taskEdges[i]);
    }
    return success;
  }

  @Override
  public Set<TE> removeAllTaskEdges(TV sourceTaskVertex,
                                    TV targetTaskVertex) {
    Set<TE> removedTaskEdge = getAllTaskEdges(sourceTaskVertex, targetTaskVertex);
    if (removedTaskEdge == null) {
      return null;
    }
    removeAllTaskEdges(removedTaskEdge);
    return removedTaskEdge;
  }

  @Override
  public boolean removeAllTaskVertices(Collection<? extends TV>
                                           taskVertices) {
    boolean flag = false;
    for (TV taskVertex : taskVertices) {
      flag |= removeTaskVertex(taskVertex);
    }
    return flag;
  }

  protected boolean assertTaskVertexExist(TV taskVertex) {
    if (containsTaskVertex(taskVertex)) {
      return true;
    } else if (taskVertex == null) {
      throw new NullPointerException();
    } else {
      throw new IllegalArgumentException(
          "no such vertex in graph: " + taskVertex.toString());
    }
  }
}



