package edu.iu.dsc.tws.task.taskgraphbuilder;

/**
 * The generic task edge factory for task vertices and task edges.
 */
public interface IDataflowTaskEdgeFactory<TV, TE> {

  /**
   * This method creates a task edge and their endpoints are source task and target task vertices.
   */
  TE createTaskEdge(TV sourceTaskVertex, TV targetTaskVertex)
      throws IllegalAccessException, InstantiationException;
}
