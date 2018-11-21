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
package edu.iu.dsc.tws.task.graph.htgraph;

import java.util.Collection;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.logging.Logger;

public class BaseHierarchicalTaskGraph<TG, TE> implements IHierarchicalTaskGraph<TG, TE> {

  private static final Logger LOG = Logger.getLogger(BaseHierarchicalTaskGraph.class.getName());

  protected Set<TG> graphs;
  protected Set<TE> edges;
  protected Set<HTGDirectedEdge<TG, TE>> htgDirectedEdges;
  protected Comparator<TG> graphComparator;
  protected Comparator<TE> edgeComparator;


  public BaseHierarchicalTaskGraph() {
  }

  public BaseHierarchicalTaskGraph(Comparator<TG> comparator, Comparator<TE> eComparator) {
    this.graphs = new HashSet<>();
    this.edges = new HashSet<>();
    this.htgDirectedEdges = new HashSet<>();
    this.graphComparator = comparator;
    this.edgeComparator = eComparator;
  }

  public boolean addTaskGraph(TG taskGraph) {
    if (taskGraph == null) {
      throw new NullPointerException();
    } else if (this.containsTaskGraph(taskGraph)) {
      return false;
    } else {
      this.graphs.add(taskGraph);
      return true;
    }
  }

  @Override
  public TE addTaskGraphEdge(TG sourceTaskGraph, TG targetTaskGraph) {

    validateTaskGraph(sourceTaskGraph);
    validateTaskGraph(targetTaskGraph);

    TE taskGraphEdge = createTaskGraphEdge(sourceTaskGraph, targetTaskGraph);

    if (containsTaskGraphEdge(taskGraphEdge)) {
      return null;
    } else {
      HTGDirectedEdge<TG, TE> directedEdge =
          createDirectedTaskGraphEdge(taskGraphEdge, sourceTaskGraph, targetTaskGraph);
      htgDirectedEdges.add(directedEdge);
      edges.add(taskGraphEdge);
      return taskGraphEdge;
    }
  }

  @Override
  public boolean addTaskGraphEdge(TG sourceTaskGraph, TG targetTaskGraph, TE taskGraphEdge) {

    if (taskGraphEdge == null) {
      throw new NullPointerException();
    } else if (containsTaskGraphEdge(taskGraphEdge)) {
      return false;
    }

    validateTaskGraph(sourceTaskGraph);
    validateTaskGraph(targetTaskGraph);

    HTGDirectedEdge<TG, TE> directedEdge =
        createDirectedTaskGraphEdge(taskGraphEdge, sourceTaskGraph, targetTaskGraph);
    edges.add(taskGraphEdge);
    htgDirectedEdges.add(directedEdge);

    return true;
  }

  private HTGDirectedEdge<TG, TE> createDirectedTaskGraphEdge(
      TE taskgraphEdge, TG sourceTaskGraph, TG targetTaskGraph) {

    HTGDirectedEdge<TG, TE> htgDirectedEdge;

    htgDirectedEdge = new HTGDirectedEdge<>();

    htgDirectedEdge.sourceTaskGraph = sourceTaskGraph;
    htgDirectedEdge.targetTaskGraph = targetTaskGraph;
    htgDirectedEdge.taskGraphEdge = taskgraphEdge;

    return htgDirectedEdge;
  }

  @Override
  public boolean containsTaskGraphEdge(TE taskgraphEdge) {
    boolean flag = false;
    for (HTGDirectedEdge<TG, TE> de : htgDirectedEdges) {
      if (edgeComparator.compare(de.taskGraphEdge, taskgraphEdge) == 0) {
        throw new RuntimeException("Duplicate task edges found for the task edge:" + taskgraphEdge);
      }
    }
    return flag;
  }

  @Override
  public boolean removeAllTaskGraphEdges(Collection<? extends TE> taskGraphEdges) {
    boolean success = false;
    for (TE taskGraphEdge : taskGraphEdges) {
      success |= removeTaskGraphEdge(taskGraphEdge);
    }
    return success;
  }

  public boolean taskGraphsEqual(TG t1, TG t2) {
    return false;
  }

  public boolean removeTaskGraphEdge(TE taskGraphEdge) {
    Iterator<HTGDirectedEdge<TG, TE>> it = htgDirectedEdges.iterator();
    while (it.hasNext()) {
      HTGDirectedEdge<TG, TE> de = it.next();
      if (edgeComparator.compare(taskGraphEdge, de.taskGraphEdge) == 0) {
        it.remove();
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean removeAllTaskGraphs(Collection<? extends TG> taskGraphs) {
    boolean flag = false;
    for (TG taskGraph : taskGraphs) {
      flag |= removeTaskGraph(taskGraph);
    }
    return flag;
  }

  public boolean removeTaskGraph(TG taskGraph) {
    Iterator<HTGDirectedEdge<TG, TE>> it = htgDirectedEdges.iterator();
    while (it.hasNext()) {
      HTGDirectedEdge<TG, TE> de = it.next();
      if (graphComparator.compare(taskGraph, de.sourceTaskGraph) == 0
          || graphComparator.compare(taskGraph, de.targetTaskGraph) == 0) {
        it.remove();
      }
    }
    return graphs.remove(taskGraph);
  }

  public Set<TE> taskGraphEdgesOf(TG taskGraph) {
    Set<TE> ret = new HashSet<>();
    Iterator<HTGDirectedEdge<TG, TE>> it = htgDirectedEdges.iterator();
    while (it.hasNext()) {
      HTGDirectedEdge<TG, TE> de = it.next();
      if (graphComparator.compare(taskGraph, de.sourceTaskGraph) == 0
          || graphComparator.compare(taskGraph, de.targetTaskGraph) == 0) {
        ret.add(de.taskGraphEdge);
      }
    }
    return ret;
  }

  public int inDegreeOfTaskGraph(TG taskGraph) {
    Set<TE> ret = new HashSet<>();
    for (HTGDirectedEdge<TG, TE> de : htgDirectedEdges) {
      if (graphComparator.compare(de.targetTaskGraph, taskGraph) == 0) {
        ret.add(de.taskGraphEdge);
      }
    }
    return ret.size();
  }


  public TE removeTaskGraphEdge(TG sourceTaskGraph, TG targetTaskGraph) {
    Iterator<HTGDirectedEdge<TG, TE>> it = htgDirectedEdges.iterator();
    while (it.hasNext()) {
      HTGDirectedEdge<TG, TE> de = it.next();
      if (graphComparator.compare(de.sourceTaskGraph, sourceTaskGraph) == 0
          && graphComparator.compare(de.targetTaskGraph, targetTaskGraph) == 0) {
        it.remove();
      }
    }
    return null;
  }

  @Override
  public Set<TE> removeAllTaskGraphEdges(TG sourceTaskGraph, TG targetTaskGraph) {
    Set<TE> removedTaskGraphEdge = getAllTaskGraphEdges(sourceTaskGraph, targetTaskGraph);
    if (removedTaskGraphEdge == null) {
      return null;
    }
    removeAllTaskGraphEdges(removedTaskGraphEdge);
    return removedTaskGraphEdge;
  }

  @Override
  public boolean containsTaskGraph(TG taskGraph) {
    return graphs.contains(taskGraph);
  }

  public TE createTaskGraphEdge(TG sourceTaskGraph, TG targetTaskGraph) {
    return null;
  }

  protected boolean validateTaskGraph(TG taskGraph) {
    if (containsTaskGraph(taskGraph)) {
      return true;
    } else if (taskGraph == null) {
      throw new NullPointerException();
    } else {
      throw new IllegalArgumentException(
          "No task graph in this hierarchical task graph: " + taskGraph.toString());
    }
  }

  @Override
  public boolean containsTaskGraphEdge(TG sourceTaskGraph, TG targetTaskGraph) {
    for (HTGDirectedEdge<TG, TE> de : htgDirectedEdges) {
      if (graphComparator.compare(de.sourceTaskGraph, sourceTaskGraph) == 0
          && graphComparator.compare(de.targetTaskGraph, targetTaskGraph) == 0) {
        return true;
      }
    }
    return false;
  }


  @Override
  public Set<TG> getTaskGraphSet() {
    return graphs;
  }

  public Set<TE> taskGraphEdgeSet() {
    return edges;
  }

  public Set<TE> getAllTaskGraphEdges(TG sourceTaskGraph, TG targetTaskGraph) {
    Set<TE> ret = new HashSet<>();
    for (HTGDirectedEdge<TG, TE> de : htgDirectedEdges) {
      if (graphComparator.compare(de.sourceTaskGraph, sourceTaskGraph) == 0
          && graphComparator.compare(de.targetTaskGraph, targetTaskGraph) == 0) {
        ret.add(de.taskGraphEdge);
      }
    }
    return ret;
  }

  public Set<TG> parentsOfTaskGraph(TG t) {
    Set<TG> ret = new HashSet<>();
    for (HTGDirectedEdge<TG, TE> de : htgDirectedEdges) {
      if (graphComparator.compare(de.targetTaskGraph, t) == 0) {
        ret.add(de.sourceTaskGraph);
      }
    }
    return ret;
  }

  public Set<TG> childrenOfTaskGraph(TG t) {
    Set<TG> ret = new HashSet<>();
    for (HTGDirectedEdge<TG, TE> de : htgDirectedEdges) {
      if (graphComparator.compare(de.sourceTaskGraph, t) == 0) {
        ret.add(de.targetTaskGraph);
      }
    }
    return ret;
  }


  public Set<TE> outgoingTaskGraphEdgesOf(TG taskGraph) {
    Set<TE> ret = new HashSet<>();
    for (HTGDirectedEdge<TG, TE> de : htgDirectedEdges) {
      if (graphComparator.compare(de.sourceTaskGraph, taskGraph) == 0) {
        ret.add(de.taskGraphEdge);
      }
    }
    return ret;
  }

  public TG connectedChildTaskGraph(TG t, TE edge) {
    Iterator<HTGDirectedEdge<TG, TE>> it = htgDirectedEdges.iterator();
    while (it.hasNext()) {
      HTGDirectedEdge<TG, TE> de = it.next();
      if (graphComparator.compare(t, de.sourceTaskGraph) == 0
          || edgeComparator.compare(de.taskGraphEdge, edge) == 0) {
        return de.targetTaskGraph;
      }
    }
    return null;
  }


  public TG connectedParentTaskGraph(TG t, TE edge) {
    Iterator<HTGDirectedEdge<TG, TE>> it = htgDirectedEdges.iterator();
    while (it.hasNext()) {
      HTGDirectedEdge<TG, TE> de = it.next();
      if (graphComparator.compare(t, de.targetTaskGraph) == 0
          || edgeComparator.compare(de.taskGraphEdge, edge) == 0) {
        return de.sourceTaskGraph;
      }
    }
    return null;
  }

  public Set<TE> incomingTaskGraphEdgesOf(TG taskGraph) {
    Set<TE> ret = new HashSet<>();
    for (HTGDirectedEdge<TG, TE> de : htgDirectedEdges) {
      if (graphComparator.compare(de.targetTaskGraph, taskGraph) == 0) {
        ret.add(de.taskGraphEdge);
      }
    }
    return ret;
  }

  public int outDegreeOfTaskGraph(TG taskGraph) {
    Set<TE> ret = new HashSet<>();
    for (HTGDirectedEdge<TG, TE> de : htgDirectedEdges) {
      if (graphComparator.compare(de.sourceTaskGraph, taskGraph) == 0) {
        ret.add(de.taskGraphEdge);
      }
    }
    return ret.size();
  }

  public void build() {
  }
}
