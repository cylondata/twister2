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

import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.logging.Logger;

/**
 * It is the base class for the dataflow task graph which implements the IDataflowTaskGraph
 * interface. This is the main interface for the directed dataflow task graph which consists of
 * methods to add the task vertices, task edges, create the directed edges, finds out the inward and
 * outward task edges, incoming and outgoing task edges.
 */
public class BaseDataflowTaskGraph<TV, TE> implements ITaskGraph<TV, TE> {
  private Set<TV> vertices;
  private Set<DirectedEdge<TV, TE>> directedEdges;

  private Comparator<TV> vertexComparator;
  private Comparator<TE> edgeComparator;

  private static final Logger LOG = Logger.getLogger(BaseDataflowTaskGraph.class.getName());

  public BaseDataflowTaskGraph() {
  }

  public BaseDataflowTaskGraph(Comparator<TV> comparator, Comparator<TE> eComparator) {
    this.vertices = new HashSet<>();
    this.directedEdges = new HashSet<>();
    this.vertexComparator = comparator;
    this.edgeComparator = eComparator;
  }

  /**
   * If the task vertex is not null and the task vertex is not available in the task vertex set,
   * it will add the task vertex to the vertices set.
   */
  public boolean addTaskVertex(TV taskVertex) {
    if (taskVertex == null) {
      throw new NullPointerException();
    } else if (this.containsTaskVertex(taskVertex)) {
      return false;
    } else {
      this.vertices.add(taskVertex);
      return true;
    }
  }

  /**
   * If the task edge is not null and the task edge is not available in the task edge set,
   * it will add the task edge to the edge set.
   */
  @Override
  public TE addTaskEdge(TV sourceTaskVertex, TV targetTaskVertex) {
    throw new UnsupportedOperationException("Undirected edge of type TE can not be returned when "
        + "adding a task edge!");
  }


  /**
   * This method first check whether the task edge is null or not, then it will validate both the
   * source and target vertex and create the directed dataflow edge between those task vertices.
   */

  @Override
  public boolean addTaskEdge(TV taskVertex1, TV taskVertex2, TE taskEdge) {
    if (taskEdge == null) {
      throw new NullPointerException();
    } else if (containsTaskEdge(taskVertex1, taskVertex2, taskEdge)) {
      return false;
    }

    validateTaskVertex(taskVertex1);
    validateTaskVertex(taskVertex2);

    DirectedEdge<TV, TE> directedEdge = new DirectedEdge<>(taskVertex1, taskVertex2, taskEdge);
    directedEdges.add(directedEdge);

    return true;
  }

  /**
   * This method returns the directed dataflow edges between the source and target task vertex
   */
  public Set<TE> getAllTaskEdges(TV sourceTaskVertex, TV targetTaskVertex) {
    Set<TE> edgeSet = new HashSet<>();
    for (DirectedEdge<TV, TE> de : directedEdges) {
      if (vertexComparator.compare(de.getSourceVertex(), sourceTaskVertex) == 0
          && vertexComparator.compare(de.getTargetVertex(), targetTaskVertex) == 0) {
        edgeSet.add(de.getTaskEdge());
      }
    }
    return edgeSet;
  }

  /**
   * This method is used to identify the duplicate task edge for the same two tasks in the graph.
   */
  @Override
  public boolean containsTaskEdge(TE taskEdge) {
    throw new UnsupportedOperationException("Unsafe method because of the DEFAULT EDGE!");
  }

  /**
   * This method is to validate whether the task graph vertices has the task vertex(TV)
   */
  @Override
  public boolean containsTaskVertex(TV taskVertex) {
    return vertices.contains(taskVertex);
  }

  /**
   * This method returns the incoming task edges of the task vertex.
   */
  public Set<TE> incomingTaskEdgesOf(TV taskVertex) {
    Set<TE> ret = new HashSet<>();
    for (DirectedEdge<TV, TE> de : directedEdges) {
      if (vertexComparator.compare(de.getTargetVertex(), taskVertex) == 0) {
        ret.add(de.getTaskEdge());
      }
    }
    return ret;
  }

  /**
   * This method returns the out degree of the task vertex (TV) if there is any edge from the
   * source task vertex to the task vertex.
   */
  public int outDegreeOfTask(TV taskVertex) {
    return outgoingTaskEdgesOf(taskVertex).size();
  }

  /**
   * This method removes the task edge between the source task vertex and the target task vertex.
   */
  public TE removeTaskEdge(TV sourceVertex, TV targetVertex) {
    Iterator<DirectedEdge<TV, TE>> it = directedEdges.iterator();
    while (it.hasNext()) {
      DirectedEdge<TV, TE> de = it.next();
      if (vertexComparator.compare(de.getSourceVertex(), sourceVertex) == 0
          && vertexComparator.compare(de.getTargetVertex(), targetVertex) == 0) {
        it.remove();
        return de.getTaskEdge();
      }
    }
    return null;
  }

  /**
   * This method removes the task edge (TE) from the task graph.
   */
  public boolean removeTaskEdge(TE taskEdge) {
    Iterator<DirectedEdge<TV, TE>> it = directedEdges.iterator();
    while (it.hasNext()) {
      DirectedEdge<TV, TE> de = it.next();
      if (edgeComparator.compare(taskEdge, de.getTaskEdge()) == 0) {
        it.remove();
        return true;
      }
    }
    return false;
  }

  /**
   * This method removes the task vertex (TV) from the task graph.
   */
  public boolean removeTaskVertex(TV taskVertex) {
    directedEdges.removeIf(de -> vertexComparator.compare(taskVertex, de.getSourceVertex()) == 0
        || vertexComparator.compare(taskVertex, de.getTargetVertex()) == 0);
    return vertices.remove(taskVertex);
  }

  /**
   * This method returns the set of task vertices in the task graph.
   */
  @Override
  public Set<TV> getTaskVertexSet() {
    return vertices;
  }

  public Set<DirectedEdge<TV, TE>> getDirectedEdgesSet() {
    return directedEdges;
  }

  /**
   * This method returns the set of task edges in the task graph.
   */
  public Set<TE> taskEdgeSet() {
    Set<TE> edges = new HashSet<>();
    directedEdges.forEach(de -> edges.add(de.getTaskEdge()));
    return edges;
  }

  /**
   * This method is used to calculate the connected child tasks between the task vertex(t) and the
   * task edge (TE)
   */
  public TV connectedChildTask(TV task, TE edge) {
    for (DirectedEdge<TV, TE> de : directedEdges) {
      if (vertexComparator.compare(task, de.getSourceVertex()) == 0
          && edgeComparator.compare(de.getTaskEdge(), edge) == 0) {
        return de.getTargetVertex();
      }
    }
    return null;
  }


  /**
   * This method returns the connected parent tasks between the task vertex(t) and the task edge(TE)
   */
  public TV connectedParentTask(TV task, TE edge) {
    for (DirectedEdge<TV, TE> de : directedEdges) {
      if (vertexComparator.compare(task, de.getTargetVertex()) == 0
          && edgeComparator.compare(de.getTaskEdge(), edge) == 0) {
        return de.getSourceVertex();
      }
    }
    return null;
  }

  /**
   * This method returns the task edge set of the task vertex (TV).
   */
  public Set<TE> taskEdgesOf(TV taskVertex) {
    Set<TE> ret = new HashSet<>();
    for (DirectedEdge<TV, TE> de : directedEdges) {
      if (vertexComparator.compare(taskVertex, de.getSourceVertex()) == 0
          || vertexComparator.compare(taskVertex, de.getTargetVertex()) == 0) {
        ret.add(de.getTaskEdge());
      }
    }
    return ret;
  }

  /**
   * This method calculates the out going task edges of the task vertex (TV). If there is an edge
   * from the source task vertex to the task Vertex (TV) then it will be added to the outgoing
   * task edge set.
   */
  public Set<TE> outgoingTaskEdgesOf(TV taskVertex) {
    Set<TE> ret = new HashSet<>();
    for (DirectedEdge<TV, TE> de : directedEdges) {
      if (vertexComparator.compare(de.getSourceVertex(), taskVertex) == 0) {
        ret.add(de.getTaskEdge());
      }
    }
    return ret;
  }

  /**
   * This method is helpful to find out the children of the task vertex (t).
   */
  public Set<TV> childrenOfTask(TV t) {
    Set<TV> ret = new HashSet<>();
    for (DirectedEdge<TV, TE> de : directedEdges) {
      if (vertexComparator.compare(de.getSourceVertex(), t) == 0) {
        ret.add(de.getTargetVertex());
      }
    }
    return ret;
  }

  /**
   * This method is helpful to find out the parents of the task vertex (t).
   */
  public Set<TV> parentsOfTask(TV t) {
    Set<TV> ret = new HashSet<>();
    for (DirectedEdge<TV, TE> de : directedEdges) {
      if (vertexComparator.compare(de.getTargetVertex(), t) == 0) {
        ret.add(de.getSourceVertex());
      }
    }
    return ret;
  }

  /**
   * This method returns the in-degree of the task vertex (TV).
   */
  public int inDegreeOfTask(TV taskVertex) {
    return incomingTaskEdgesOf(taskVertex).size();
  }

  /**
   * This method is to validate that whether the source and target task vertex has task edge.
   */
  @Override
  public boolean containsTaskEdge(TV sourceTaskVertex, TV targetTaskVertex) {
    for (DirectedEdge<TV, TE> de : directedEdges) {
      if (vertexComparator.compare(de.getSourceVertex(), sourceTaskVertex) == 0
          && vertexComparator.compare(de.getTargetVertex(), targetTaskVertex) == 0) {
        return true;
      }
    }
    return false;
  }

  public boolean containsTaskEdge(TV sourceTaskVertex, TV targetTaskVertex, TE taskEdge) {
    for (DirectedEdge<TV, TE> de : directedEdges) {
      if (edgeComparator.compare(de.getTaskEdge(), taskEdge) == 0
          && vertexComparator.compare(de.getSourceVertex(), sourceTaskVertex) == 0
          && vertexComparator.compare(de.getTargetVertex(), targetTaskVertex) == 0
      ) {
        return true;
      }
    }
    return false;
  }

  /**
   * This method remove all the task edges in the collection object.
   */
  @Override
  public boolean removeAllTaskEdges(Collection<? extends TE> taskEdges) {
    boolean success = false;
    for (TE taskEdge : taskEdges) {
      success |= removeTaskEdge(taskEdge);
    }
    return success;
  }

  /**
   * This method remove all the task vertices in the collection object.
   */
  @Override
  public boolean removeAllTaskVertices(Collection<? extends TV>
                                           taskVertices) {
    boolean flag = false;
    for (TV taskVertex : taskVertices) {
      flag |= removeTaskVertex(taskVertex);
    }
    return flag;
  }

  /**
   * This method is responsible for removing all the task edges between the source and target
   * task vertex.
   */
  @Override
  public Set<TE> removeAllTaskEdges(TV sourceTaskVertex, TV targetTaskVertex) {
    Set<TE> removedTaskEdges = new HashSet<>();

    for (DirectedEdge<TV, TE> de : directedEdges) {
      if (vertexComparator.compare(de.getSourceVertex(), sourceTaskVertex) == 0
          && vertexComparator.compare(de.getTargetVertex(), targetTaskVertex) == 0
      ) {
        removedTaskEdges.add(de.getTaskEdge());
        directedEdges.remove(de);
      }
    }

    return removedTaskEdges;
  }

  //future use
  public DirectedEdge<TV, TE> getDataflowTaskEdge(TE taskEdge) {
    return null;
  }

  /**
   * This method validates the task graph to finds that if there is a self-loop in the task
   * vertex set (task graph)
   */
  public boolean validate() {
    //call to check the self-loop
    return !detectSelfLoop();
  }

  /**
   * This method validates that whether the task graph contains the task vertex (TV).
   */
  protected void validateTaskVertex(TV taskVertex) {
    if (!containsTaskVertex(taskVertex)) {
      throw new RuntimeException("No task vertex in this task graph: " + taskVertex.toString());
    }
  }

  /**
   * This method identifies the self-loop for the task vertex set (Set<TV>) in the task graph.
   */
  private boolean detectSelfLoop() {
    for (DirectedEdge<TV, TE> de : directedEdges) {
      if (de.getSourceVertex().equals(de.getTargetVertex())) {
        throw new RuntimeException(String.format("Self-loop detected for the task graph in edge "
            + "%s between %s and %s", de.getTaskEdge(), de.getSourceVertex(),
            de.getTargetVertex()));
      }
    }
    return true;
  }

  /**
   * This method gets the complete task vertex of the task graph and call
   * the detect cycle method to identify if there is a cycle in the  task graph.
   *
   * @return true/false
   */
  public boolean hasCycle() {
    Set<TV> taskVertexSet = getTaskVertexSet();
    Set<TV> sourceTaskVertex = new HashSet<>();
    Set<TV> targetTaskVertex = new HashSet<>();

    while (taskVertexSet.size() > 0) {
      TV taskVertex = taskVertexSet.iterator().next();
      if (detectCycle(taskVertex, taskVertexSet, sourceTaskVertex, targetTaskVertex)) {
        throw new RuntimeException("Cycle is detected for the vertex:" + taskVertex);
      }
    }
    return false;
  }

  /**
   * This is the recursive loop to traverse the complete task graph and
   * identify the cycles in the loop.
   *
   * @return true/false
   */
  private boolean detectCycle(TV vertex, Set<TV> taskVertexSet,
                              Set<TV> sourceTaskSet, Set<TV> targetTaskSet) {
    traversedVertex(vertex, taskVertexSet, sourceTaskSet);
    for (TV childTask : childrenOfTask(vertex)) {
      if (sourceTaskSet.contains(childTask)) {
        return true;
      }
      if (detectCycle(childTask, taskVertexSet, sourceTaskSet, targetTaskSet)) {
        return true;
      }
    }
    traversedVertex(vertex, sourceTaskSet, targetTaskSet);
    return false;
  }

  /**
   * Mark the visited vertexes and store it in the target task vertex set and
   * remove it from the source target task vertex set
   */
  private void traversedVertex(TV vertex, Set<TV> sourceTaskSet, Set<TV> targetTaskSet) {
    sourceTaskSet.remove(vertex);
    targetTaskSet.add(vertex);
  }

  /**
   * Build the internal structures of the graph, so that it can be searched
   */
  public void build() {
  }
}
