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
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.logging.Logger;

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
    this.vertices = new LinkedHashSet<>();
    this.edges = new HashSet<>();
    this.directedEdges = new HashSet<>();
    this.vertexComparator = comparator;
    this.edgeComparator = eComparator;
  }

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

  private DirectedEdge<TV, TE> createDirectedDataflowTaskEdge(
      TE taskEdge, TV sourceTaskVertex, TV targetTaskVertex) {

    DirectedEdge<TV, TE> directedEdge;
    directedEdge = new DirectedEdge<TV, TE>();

    directedEdge.sourceTaskVertex = sourceTaskVertex;
    directedEdge.targetTaskVertex = targetTaskVertex;
    directedEdge.taskEdge = taskEdge;

    return directedEdge;
  }

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

  /* Commented for duplicate task edge validation for same tasks.
  @Override
  public boolean containsTaskEdge(TE taskEdge) {
    return edges.contains(taskEdge);
  }*/

  /**
   * This method is used to identify the duplicate task edge for the
   * same two tasks in the graph.
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

  /* Commented for duplicate task vertex names in the graph. */
  @Override
  public boolean containsTaskVertex(TV taskVertex) {
    return vertices.contains(taskVertex);
  }

  public Set<TE> incomingTaskEdgesOf(TV taskVertex) {
    Set<TE> ret = new HashSet<>();
    for (DirectedEdge<TV, TE> de : directedEdges) {
      if (vertexComparator.compare(de.targetTaskVertex, taskVertex) == 0) {
        ret.add(de.taskEdge);
      }
    }
    return ret;
  }

  public int outDegreeOfTask(TV taskVertex) {
    Set<TE> ret = new HashSet<>();
    for (DirectedEdge<TV, TE> de : directedEdges) {
      if (vertexComparator.compare(de.sourceTaskVertex, taskVertex) == 0) {
        ret.add(de.taskEdge);
      }
    }
    return ret.size();
  }

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

  @Override
  public Set<TV> getTaskVertexSet() {
    return vertices;
  }

  public Set<TE> taskEdgeSet() {
    return edges;
  }

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

  public Set<TE> outgoingTaskEdgesOf(TV taskVertex) {
    Set<TE> ret = new HashSet<>();
    for (DirectedEdge<TV, TE> de : directedEdges) {
      if (vertexComparator.compare(de.sourceTaskVertex, taskVertex) == 0) {
        ret.add(de.taskEdge);
      }
    }
    return ret;
  }

  public Set<TV> childrenOfTask(TV t) {
    Set<TV> ret = new LinkedHashSet<>();
    for (DirectedEdge<TV, TE> de : directedEdges) {
      if (vertexComparator.compare(de.sourceTaskVertex, t) == 0) {
        ret.add(de.targetTaskVertex);
      }
    }
    return ret;
  }

  public Set<TV> parentsOfTask(TV t) {
    Set<TV> ret = new LinkedHashSet<>();
    for (DirectedEdge<TV, TE> de : directedEdges) {
      if (vertexComparator.compare(de.targetTaskVertex, t) == 0) {
        ret.add(de.sourceTaskVertex);
      }
    }
    return ret;
  }

  public int inDegreeOfTask(TV taskVertex) {
    Set<TE> ret = new HashSet<>();
    for (DirectedEdge<TV, TE> de : directedEdges) {
      if (vertexComparator.compare(de.targetTaskVertex, taskVertex) == 0) {
        ret.add(de.taskEdge);
      }
    }
    return ret.size();
  }

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

  @Override
  public boolean removeAllTaskEdges(Collection<? extends TE> taskEdges) {
    boolean success = false;
    for (TE taskEdge : taskEdges) {
      success |= removeTaskEdge(taskEdge);
    }
    return success;
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

  public DirectedEdge<TV, TE> getDataflowTaskEdge(TE taskEdge) {
    return null;
  }

  /**
   * Validate the graph and check weather there are invalid entries
   * @return true if the graph is valid
   *
   */
  public boolean validate() {
    //return true;

    //Code to check the self-loop
    boolean flag = false;
    if (!detectSelfLoop(getTaskVertexSet())) {
      return true;
    }
    return flag;
  }

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


  public boolean containsSelfLoop(TV sourceTaskVertex) {

    boolean flag = false;
    for (DirectedEdge<TV, TE> de : directedEdges) {
      if (de.sourceTaskVertex.equals(de.targetTaskVertex)) {
        throw new RuntimeException("Self-loop detected for the taskgraph");
      }
    }
    return flag;
  }

  public boolean detectCycle(DataFlowTaskGraph dataFlowTaskGraph, Set<TV> taskVertex) {

    boolean flag = false;
    Iterator<TV> vertexIterator = taskVertex.iterator();
    while (vertexIterator.hasNext()) {
      if (containsCycle(vertexIterator.next())) {
        flag = true;
      }
    }
    return flag;
  }

  public boolean containsCycle(TV sourceTaskVertex) {

    for (DirectedEdge<TV, TE> de : directedEdges) {
     //flag = true;
     //throw new RuntimeException ("Self-loop detected for the taskgraph:");
    }
    return false;
  }

  /**
   * Build the internal structures of the graph, so that it can be searched
   */
  public void build() {
  }

}



