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
import java.util.logging.Logger;

public class HierarchicalTaskGraph extends BaseHierarchicalTaskGraph<DataFlowTaskGraph, Edge> {

  private static final Logger LOG = Logger.getLogger(HierarchicalTaskGraph.class.getName());

  private Map<String, DataFlowTaskGraph> taskGraphMap = new HashMap<>();
  private Set<DataFlowTaskGraph> taskGraph = new HashSet<>();

  private OperationMode operationMode = OperationMode.STREAMING;

  public HierarchicalTaskGraph() {
    super(Comparator.comparing(DataFlowTaskGraph::getTaskGraphName),
        Comparator.comparing(Edge::getName));
  }

  public HierarchicalTaskGraph(OperationMode mode) {
    super(Comparator.comparing(DataFlowTaskGraph::getTaskGraphName),
        Comparator.comparing(Edge::getName));
    this.operationMode = mode;
  }

  public void setOperationMode(OperationMode operationMode) {
    this.operationMode = operationMode;
  }

  @Override
  public void build() {
    for (HTGDirectedEdge<DataFlowTaskGraph, Edge> de : htgDirectedEdges) {
      taskGraphMap.put(de.sourceTaskGraph.getTaskGraphName(), de.sourceTaskGraph);
      taskGraphMap.put(de.targetTaskGraph.getTaskGraphName(), de.targetTaskGraph);
    }
  }

  public DataFlowTaskGraph dataFlowTaskGraph(String name) {
    return taskGraphMap.get(name);
  }

  public DataFlowTaskGraph graph(String name) {
    return taskGraphMap.get(name);
  }

  public void addTaskGraph(String name, DataFlowTaskGraph dataFlowTaskGraph) {
    if (!validateTaskGraph(name)) {
      addTaskGraph(dataFlowTaskGraph);
      taskGraphMap.put(name, dataFlowTaskGraph);
    }
  }

  private boolean validateTaskGraph(String taskgraphName) {
    boolean flag = false;
    if (taskGraphMap.containsKey(taskgraphName)) {
      throw new RuntimeException("Duplicate names for the submitted graph:" + taskgraphName);
    }
    return flag;
  }

  public Set<DataFlowTaskGraph> childrenOfTaskGraph(String taskgraphName) {
    DataFlowTaskGraph t = taskGraphMap.get(taskgraphName);
    if (t == null) {
      return new HashSet<>();
    }
    return childrenOfTaskGraph(t);
  }

  public Set<DataFlowTaskGraph> parentsOfTaskGraph(String taskName) {
    DataFlowTaskGraph t = taskGraphMap.get(taskName);
    if (t == null) {
      return new HashSet<>();
    }
    return parentsOfTaskGraph(t);
  }

  public DataFlowTaskGraph childOfTaskGraph(DataFlowTaskGraph taskgraph, String edge) {
    Set<Edge> edges = outEdges(taskgraph);

    Edge taskEdge = null;
    for (Edge e : edges) {
      if (e.getName().equals(edge)) {
        taskEdge = e;
      }
    }

    if (taskEdge != null) {
      return connectedChildTaskGraph(taskgraph, taskEdge);
    } else {
      return null;
    }
  }

  public DataFlowTaskGraph getParentOfTaskGraph(DataFlowTaskGraph taskgraph, String edge) {
    Set<Edge> edges = inEdges(taskgraph);

    Edge taskEdge = null;
    for (Edge e : edges) {
      if (e.getName().equals(edge)) {
        taskEdge = e;
      }
    }

    if (taskEdge != null) {
      return connectedParentTaskGraph(taskgraph, taskEdge);
    } else {
      return null;
    }
  }

  public Set<Edge> outEdges(DataFlowTaskGraph taskgraph) {
    return outgoingTaskGraphEdgesOf(taskgraph);
  }


  public Set<Edge> outEdges(String taskName) {
    DataFlowTaskGraph t = taskGraphMap.get(taskName);
    if (t == null) {
      return new HashSet<>();
    }
    return outEdges(t);
  }


  public Set<Edge> inEdges(DataFlowTaskGraph taskgraph) {
    return incomingTaskGraphEdgesOf(taskgraph);
  }


  public Set<Edge> inEdges(String taskName) {
    DataFlowTaskGraph t = taskGraphMap.get(taskName);
    if (t == null) {
      return new HashSet<>();
    }
    return inEdges(t);
  }

  private static class GraphComparator implements Comparator<DataFlowTaskGraph> {

    @Override
    public int compare(DataFlowTaskGraph o1, DataFlowTaskGraph o2) {
      return new HierarchicalTaskGraph.StringComparator().compare(o1.getTaskGraphName(),
          o2.getTaskGraphName());
    }
  }

  private static class EdgeComparator implements Comparator<Edge> {

    @Override
    public int compare(Edge o1, Edge o2) {
      return new HierarchicalTaskGraph.StringComparator().compare(o1.getName(), o2.getName());
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
