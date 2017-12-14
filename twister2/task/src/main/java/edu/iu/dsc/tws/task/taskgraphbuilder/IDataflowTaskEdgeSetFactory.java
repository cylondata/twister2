package edu.iu.dsc.tws.task.taskgraphbuilder;

import java.util.Set;

/**
 * The generic task edge factory for task vertices and task edges.
 */
public interface IDataflowTaskEdgeSetFactory<TV, TE> {

  /**
   * This method creates a set of task edge for the endpoint 'taskVertex'.
   */
  Set<TE> createTaskEdgeSet(TV taskVertex) throws IllegalAccessException, InstantiationException;
}
