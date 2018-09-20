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

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import edu.iu.dsc.tws.task.api.ICompute;

/**
 * This class extends the base data flow task graph which is mainly responsible for building the
 * task graph for the task vertex and the task edge.
 */
public class DataFlowTaskGraph extends BaseDataflowTaskGraph<Vertex, Edge> {
  private Map<String, Vertex> taskMap = new HashMap<>();

  private OperationMode operationMode = OperationMode.STREAMING;

  public DataFlowTaskGraph() {
    //super(new VertexComparator(), new EdgeComparator());
    super(Comparator.comparing(Vertex::getName), Comparator.comparing(Edge::getName));
  }

  public DataFlowTaskGraph(OperationMode mode) {
    //super(new VertexComparator(), new EdgeComparator());
    super(Comparator.comparing(Vertex::getName), Comparator.comparing(Edge::getName));
    this.operationMode = mode;
  }

  /**
   * This method is responsible for storing the directed edges between the source and target task
   * vertex in a map.
   */
  @Override
  public void build() {
    validate();

    Set<ICompute> ret = new HashSet<>();
    for (DirectedEdge<Vertex, Edge> de : directedEdges) {
      taskMap.put(de.sourceTaskVertex.getName(), de.sourceTaskVertex);
      taskMap.put(de.targetTaskVertex.getName(), de.targetTaskVertex);
    }
  }

  /**
   * This method is responsible for adding the task vertex to the task map.
   * @param name
   * @param taskVertex
   * @return
   */
  public boolean addTaskVertex(String name, Vertex taskVertex) {
    if (!validateTaskVertex(name)) {
      addTaskVertex(taskVertex);
      taskMap.put(name, taskVertex);
    }
    return true;
  }

  /**
   * This method is to identify the duplicate names for the tasks in the taskgraph.
   * @param taskName
   * @return
   */
  private boolean validateTaskVertex(String taskName) {
    boolean flag = false;
    if (taskMap.containsKey(taskName)) {
      throw new RuntimeException("Duplicate names for the submitted task:" + taskName);
    }
    return flag;
  }

  public Vertex vertex(String name) {
    return taskMap.get(name);
  }

  public Set<Edge> outEdges(Vertex task) {
    return outgoingTaskEdgesOf(task);
  }

  public Set<Edge> outEdges(String taskName) {
    Vertex t = taskMap.get(taskName);
    if (t == null) {
      return new HashSet<>();
    }
    return outEdges(t);
  }

  public Set<Edge> inEdges(Vertex task) {
    return incomingTaskEdgesOf(task);
  }

  public Set<Edge> inEdges(String taskName) {
    Vertex t = taskMap.get(taskName);
    if (t == null) {
      return new HashSet<>();
    }
    return inEdges(t);
  }

  public Set<Vertex> childrenOfTask(String taskName) {
    Vertex t = taskMap.get(taskName);
    if (t == null) {
      return new HashSet<>();
    }
    return childrenOfTask(t);
  }

  public Set<Vertex> parentsOfTask(String taskName) {
    Vertex t = taskMap.get(taskName);
    if (t == null) {
      return new HashSet<>();
    }
    return parentsOfTask(t);
  }

  public Vertex childOfTask(Vertex task, String edge) {
    Set<Edge> edges = outEdges(task);

    Edge taskEdge = null;
    for (Edge e : edges) {
      if (e.getName().equals(edge)) {
        taskEdge = e;
      }
    }

    if (taskEdge != null) {
      return connectedChildTask(task, taskEdge);
    } else {
      return null;
    }
  }

  public Vertex getParentOfTask(Vertex task, String edge) {
    Set<Edge> edges = inEdges(task);

    Edge taskEdge = null;
    for (Edge e : edges) {
      if (e.getName().equals(edge)) {
        taskEdge = e;
      }
    }

    if (taskEdge != null) {
      return connectedParentTask(task, taskEdge);
    } else {
      return null;
    }
  }

  /**
   * This is the getter method to get the property of operation mode "STREAMING" or "BATCH".
   * @return
   */
  public OperationMode getOperationMode() {
    return operationMode;
  }

  /**
   * This is the setter method to set the property of the operation mode which is either
   * "STREAMING" or "BATCH"
   * @param operationMode
   */
  public void setOperationMode(OperationMode operationMode) {
    this.operationMode = operationMode;
  }

  private static class VertexComparator implements Comparator<Vertex> {

    @Override
    public int compare(Vertex o1, Vertex o2) {
      return new StringComparator().compare(o1.getName(), o2.getName());
    }
  }

  private static class EdgeComparator implements Comparator<Edge> {

    @Override
    public int compare(Edge o1, Edge o2) {
      return new StringComparator().compare(o1.getName(), o2.getName());
    }
  }

  public static class StringComparator implements Comparator<String> {

    public Comparator<String> comp(String obj1, String obj2) {
      Comparator<String> stringComparator = Comparator.comparing(String::toString);
      return stringComparator;
    }

    public int compare(String obj1, String obj2) {
      if (obj1 == null) {
        return -1;
      }
      if (obj2 == null) {
        return 1;
      }
      if (obj1.equals(obj2)) {
        return 0;
      }
      return obj1.compareTo(obj2);
    }
  }
}
