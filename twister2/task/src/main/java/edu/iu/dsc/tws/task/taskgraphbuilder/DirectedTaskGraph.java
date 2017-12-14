package edu.iu.dsc.tws.task.taskgraphbuilder;

import java.util.Set;

/**
 * This is the main interface for directed task graph.
 */
public interface DirectedTaskGraph<TV, TE> extends ITaskGraph<TV, TE> {

  /**
   * This method is responsible for returning the number of inward directed edges for the task vertex 'TV'
   */
  int inDegreeOf(TV taskVertex);

  /**
   * This method returns the set of incoming task edges for the task vertex 'TV'
   */
  Set<TE> incomingTaskEdgesOf(TV taskVertex);

  /**
   * This method returns the set of outward task edges for the task vertex 'TV'
   */
  int outDegreeOf(TV taskVertex);

  /**
   * This method returns the set of outgoing task edges for the task vertex 'TV'
   */
  Set<TE> outgoingTaskEdgesOf(TV taskVertex);

}
