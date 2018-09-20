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
 * @param <TV>
 * @param <TE>
 */
public class BaseDataflowTaskGraph<TV, TE> implements ITaskGraph<TV, TE> {
  protected Set<TV> vertices;
  protected Set<TE> edges;
  protected Set<DirectedEdge<TV, TE>> directedEdges;
  protected Comparator<TV> vertexComparator;
  protected Comparator<TE> edgeComparator;

  private static final Logger LOG = Logger.getLogger(BaseDataflowTaskGraph.class.getName());

  public BaseDataflowTaskGraph() {
  }

  public BaseDataflowTaskGraph(Comparator<TV> comparator, Comparator<TE> eComparator) {
    this.vertices = new HashSet<>();
    this.edges = new HashSet<>();
    this.directedEdges = new HashSet<>();
    this.vertexComparator = comparator;
    this.edgeComparator = eComparator;
  }

  /**
   * If the task vertex is not null and the task vertex is not available in the task vertex set,
   * it will add the task vertex to the vertices set.
   * @param taskVertex
   * @return
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
   * @param sourceTaskVertex
   * @param targetTaskVertex
   * @return
   */
  @Override
  public TE addTaskEdge(TV sourceTaskVertex, TV targetTaskVertex) {

    validateTaskVertex(sourceTaskVertex);
    validateTaskVertex(targetTaskVertex);

    TE taskEdge = createEdge(sourceTaskVertex, targetTaskVertex);

    if (containsTaskEdge(taskEdge)) {
      return null;
    } else {
      DirectedEdge<TV, TE> directedEdge =
          createDirectedDataflowTaskEdge(taskEdge, sourceTaskVertex, targetTaskVertex);
      directedEdges.add(directedEdge);
      edges.add(taskEdge);
      return taskEdge;
    }
  }

  public TE createEdge(TV sourceTaskVertex, TV targetTaskVertex) {
    return null;
  }

  public boolean tasksEqual(TV t1, TV t2) {
    return false;
  }

  /**
   * This method first check whether the task edge is null or not, then it will validate both the
   * source and target vertex and create the directed dataflow edge between those task vertices.
   * @param taskVertex1
   * @param taskVertex2
   * @param taskEdge
   * @return
   */

  @Override
  public boolean addTaskEdge(TV taskVertex1, TV taskVertex2, TE taskEdge) {
    if (taskEdge == null) {
      throw new NullPointerException();
    } else if (containsTaskEdge(taskEdge)) {
      return false;
    }

    validateTaskVertex(taskVertex1);
    validateTaskVertex(taskVertex2);

    DirectedEdge<TV, TE> directedEdge =
        createDirectedDataflowTaskEdge(taskEdge, taskVertex1, taskVertex2);
    edges.add(taskEdge);
    directedEdges.add(directedEdge);

    return true;
  }

  /**
   * This method assign the directed dataflow edge between the source and target task vertex
   * @param taskEdge
   * @param sourceTaskVertex
   * @param targetTaskVertex
   * @return
   */
  private DirectedEdge<TV, TE> createDirectedDataflowTaskEdge(
      TE taskEdge, TV sourceTaskVertex, TV targetTaskVertex) {

    DirectedEdge<TV, TE> directedEdge;

    directedEdge = new DirectedEdge<>();

    directedEdge.sourceTaskVertex = sourceTaskVertex;
    directedEdge.targetTaskVertex = targetTaskVertex;
    directedEdge.taskEdge = taskEdge;

    return directedEdge;
  }

  /**
   * This method returns the directed dataflow edges between the source and target task vertex
   * @param sourceTaskVertex
   * @param targetTaskVertex
   * @return
   */
  public Set<TE> getAllTaskEdges(TV sourceTaskVertex, TV targetTaskVertex) {
    Set<TE> ret = new HashSet<>();
    for (DirectedEdge<TV, TE> de : directedEdges) {
      if (vertexComparator.compare(de.sourceTaskVertex, sourceTaskVertex) == 0
          && vertexComparator.compare(de.targetTaskVertex, targetTaskVertex) == 0) {
        ret.add(de.taskEdge);
      }
    }
    return ret;
  }

  /**
   * This method is used to identify the duplicate task edge for the same two tasks in the graph.
   * @param taskEdge
   * @return
   */
  @Override
  public boolean containsTaskEdge(TE taskEdge) {

    boolean flag = false;
    for (DirectedEdge<TV, TE> de : directedEdges) {
      if (edgeComparator.compare(de.taskEdge, taskEdge) == 0) {
        //flag = true;
        throw new RuntimeException("Duplicate task edges found for the task edge:" + taskEdge);
      }
    }
    return flag;
  }

  /**
   * This method is to validate whether the task graph vertices has the task vertex(TV)
   * @param taskVertex
   * @return
   */
  @Override
  public boolean containsTaskVertex(TV taskVertex) {
    return vertices.contains(taskVertex);
  }

  /**
   * This method returns the incoming task edges of the task vertex.
   * @param taskVertex
   * @return
   */
  public Set<TE> incomingTaskEdgesOf(TV taskVertex) {
    Set<TE> ret = new HashSet<>();
    for (DirectedEdge<TV, TE> de : directedEdges) {
      if (vertexComparator.compare(de.targetTaskVertex, taskVertex) == 0) {
        ret.add(de.taskEdge);
      }
    }
    return ret;
  }

  /**
   * This method returns the out degree of the task vertex (TV) if there is any edge from the
   * source task vertex to the task vertex.
   * @param taskVertex
   * @return
   */
  public int outDegreeOfTask(TV taskVertex) {
    Set<TE> ret = new HashSet<>();
    for (DirectedEdge<TV, TE> de : directedEdges) {
      if (vertexComparator.compare(de.sourceTaskVertex, taskVertex) == 0) {
        ret.add(de.taskEdge);
      }
    }
    return ret.size();
  }

  /**
   * This method removes the task edge between the source task vertex and the target task vertex.
   * @param sourceVertex
   * @param targetVertex
   * @return
   */
  public TE removeTaskEdge(TV sourceVertex, TV targetVertex) {
    Iterator<DirectedEdge<TV, TE>> it = directedEdges.iterator();
    while (it.hasNext()) {
      DirectedEdge<TV, TE> de = it.next();
      if (vertexComparator.compare(de.sourceTaskVertex, sourceVertex) == 0
          && vertexComparator.compare(de.targetTaskVertex, targetVertex) == 0) {
        it.remove();
      }
    }
    return null;
  }

  /**
   * This method removes the task edge (TE) from the task graph.
   * @param taskEdge
   * @return
   */
  public boolean removeTaskEdge(TE taskEdge) {
    Iterator<DirectedEdge<TV, TE>> it = directedEdges.iterator();
    while (it.hasNext()) {
      DirectedEdge<TV, TE> de = it.next();
      if (edgeComparator.compare(taskEdge, de.taskEdge) == 0) {
        it.remove();
        return true;
      }
    }
    return false;
  }

  /**
   * This method removes the task vertex (TV) from the task graph.
   * @param taskVertex
   * @return
   */
  public boolean removeTaskVertex(TV taskVertex) {
    Iterator<DirectedEdge<TV, TE>> it = directedEdges.iterator();
    while (it.hasNext()) {
      DirectedEdge<TV, TE> de = it.next();
      if (vertexComparator.compare(taskVertex, de.sourceTaskVertex) == 0
          || vertexComparator.compare(taskVertex, de.targetTaskVertex) == 0) {
        it.remove();
      }
    }
    return vertices.remove(taskVertex);
  }

  /**
   * This method returns the set of task vertices in the task graph.
   * @return
   */
  @Override
  public Set<TV> getTaskVertexSet() {
    return vertices;
  }

  /**
   * This method returns the set of task edges in the task graph.
   * @return
   */
  public Set<TE> taskEdgeSet() {
    return edges;
  }

  /**
   * This method is used to calculate the connected child tasks between the task vertex(t) and the
   * task edge (TE)
   * @param t
   * @param edge
   * @return
   */
  public TV connectedChildTask(TV t, TE edge) {
    Iterator<DirectedEdge<TV, TE>> it = directedEdges.iterator();
    while (it.hasNext()) {
      DirectedEdge<TV, TE> de = it.next();
      if (vertexComparator.compare(t, de.sourceTaskVertex) == 0
          || edgeComparator.compare(de.taskEdge, edge) == 0) {
        return de.targetTaskVertex;
      }
    }
    return null;
  }


  /**
   * This method returns the connected parent tasks between the task vertex(t) and the task edge(TE)
   * @param t
   * @param edge
   * @return
   */
  public TV connectedParentTask(TV t, TE edge) {
    Iterator<DirectedEdge<TV, TE>> it = directedEdges.iterator();
    while (it.hasNext()) {
      DirectedEdge<TV, TE> de = it.next();
      if (vertexComparator.compare(t, de.targetTaskVertex) == 0
          || edgeComparator.compare(de.taskEdge, edge) == 0) {
        return de.sourceTaskVertex;
      }
    }
    return null;
  }

  /**
   * This method returns the task edge set of the task vertex (TV).
   * @param taskVertex
   * @return
   */
  public Set<TE> taskEdgesOf(TV taskVertex) {
    Set<TE> ret = new HashSet<>();
    Iterator<DirectedEdge<TV, TE>> it = directedEdges.iterator();
    while (it.hasNext()) {
      DirectedEdge<TV, TE> de = it.next();
      if (vertexComparator.compare(taskVertex, de.sourceTaskVertex) == 0
          || vertexComparator.compare(taskVertex, de.targetTaskVertex) == 0) {
        ret.add(de.taskEdge);
      }
    }
    return ret;
  }

  /**
   * This method calculates the out going task edges of the task vertex (TV). If there is an edge
   * from the source task vertex to the task Vertex (TV) then it will be added to the outgoing
   * task edge set.
   * @param taskVertex
   * @return
   */
  public Set<TE> outgoingTaskEdgesOf(TV taskVertex) {
    Set<TE> ret = new HashSet<>();
    for (DirectedEdge<TV, TE> de : directedEdges) {
      if (vertexComparator.compare(de.sourceTaskVertex, taskVertex) == 0) {
        ret.add(de.taskEdge);
      }
    }
    return ret;
  }

  /**
   * This method is helpful to find out the children of the task vertex (t).
   * @param t
   * @return
   */
  public Set<TV> childrenOfTask(TV t) {
    Set<TV> ret = new HashSet<>();
    for (DirectedEdge<TV, TE> de : directedEdges) {
      if (vertexComparator.compare(de.sourceTaskVertex, t) == 0) {
        ret.add(de.targetTaskVertex);
      }
    }
    return ret;
  }

  /**
   * This method is helpful to find out the parents of the task vertex (t).
   * @param t
   * @return
   */
  public Set<TV> parentsOfTask(TV t) {
    Set<TV> ret = new HashSet<>();
    for (DirectedEdge<TV, TE> de : directedEdges) {
      if (vertexComparator.compare(de.targetTaskVertex, t) == 0) {
        ret.add(de.sourceTaskVertex);
      }
    }
    return ret;
  }

  /**
   * This method returns the in-degree of the task vertex (TV).
   * @param taskVertex
   * @return
   */
  public int inDegreeOfTask(TV taskVertex) {
    Set<TE> ret = new HashSet<>();
    for (DirectedEdge<TV, TE> de : directedEdges) {
      if (vertexComparator.compare(de.targetTaskVertex, taskVertex) == 0) {
        ret.add(de.taskEdge);
      }
    }
    return ret.size();
  }

  /**
   * This method is to validate that whether the source and target task vertex has task edge.
   * @param sourceTaskVertex
   * @param targetTaskVertex
   * @return
   */
  @Override
  public boolean containsTaskEdge(TV sourceTaskVertex,
                                  TV targetTaskVertex) {
    for (DirectedEdge<TV, TE> de : directedEdges) {
      if (vertexComparator.compare(de.sourceTaskVertex, sourceTaskVertex) == 0
          && vertexComparator.compare(de.targetTaskVertex, targetTaskVertex) == 0) {
        return true;
      }
    }
    return false;
  }

  /**
   * This method remove all the task edges in the collection object.
   * @param taskEdges
   * @return
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
   * @param taskVertices
   * @return
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
   * @param sourceTaskVertex
   * @param targetTaskVertex
   * @return
   */
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

  //future use
  public DirectedEdge<TV, TE> getDataflowTaskEdge(TE taskEdge) {
    return null;
  }

  /**
   * This method validates the task graph to finds that if there is a self-loop in the task
   * vertex set (task graph)
   * @return
   */
  public boolean validate() {
    //call to check the self-loop
    boolean flag = false;
    if (!detectSelfLoop(getTaskVertexSet())) {
      return true;
    }
    return flag;
  }

  /**
   * This method validates that whether the task graph contains the task vertex (TV).
   * @param taskVertex
   * @return
   */
  protected boolean validateTaskVertex(TV taskVertex) {
    if (containsTaskVertex(taskVertex)) {
      return true;
    } else if (taskVertex == null) {
      throw new NullPointerException();
    } else {
      throw new IllegalArgumentException(
          "No task vertex in this task graph: " + taskVertex.toString());
    }
  }

  /**
   * This method validates that whether the task graph contains the task edge (TE).
   * @param taskEdge
   * @return
   */
  protected boolean validateTaskEdges(TE taskEdge) {
    if (containsTaskEdge(taskEdge)) {
      return true;
    } else if (taskEdge == null) {
      throw new NullPointerException();
    } else {
      throw new IllegalArgumentException(
          "No task Edge in this task graph: " + taskEdge.toString());
    }
  }

  /**
   * This method identifies the self-loop for the task vertex set (Set<TV>) in the task graph.
   * @param taskVertex
   * @return
   */
  public boolean detectSelfLoop(Set<TV> taskVertex) {
    boolean flag = false;
    Iterator<TV> vertexIterator = taskVertex.iterator();
    while (vertexIterator.hasNext()) {
      if (!containsSelfLoop(vertexIterator.next())) {
        flag = true;
      }
    }
    return flag;
  }

  /**
   * This method validates the self-loop for a particular task vertex (TV)
   * @param sourceTaskVertex
   * @return
   */
  private boolean containsSelfLoop(TV sourceTaskVertex) {
    boolean flag = false;
    for (DirectedEdge<TV, TE> de : directedEdges) {
      if (de.sourceTaskVertex.equals(de.targetTaskVertex)) {
        throw new RuntimeException("Self-loop detected for the taskgraph");
      }
    }
    return flag;
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
   * @param vertex
   * @param taskVertexSet
   * @param sourceTaskSet
   * @param targetTaskSet
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
   * @param vertex
   * @param sourceTaskSet
   * @param targetTaskSet
   */
  private void traversedVertex(TV vertex, Set<TV> sourceTaskSet,
                               Set<TV> targetTaskSet) {
    sourceTaskSet.remove(vertex);
    targetTaskSet.add(vertex);
  }

  /**
   * Build the internal structures of the graph, so that it can be searched
   */
  public void build() {
  }
}




