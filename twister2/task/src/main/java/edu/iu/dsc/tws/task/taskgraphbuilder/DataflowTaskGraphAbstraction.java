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
package edu.iu.dsc.tws.task.taskgraphbuilder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * It is an abstract implementation of the task graph class which is mainly responsible for creating
 * the task edge factory, task vertex and task edge map.
 */
public abstract class DataflowTaskGraphAbstraction<TV, TE>
    implements ITaskGraph<TV, TE> {

  private IDataflowTaskEdgeFactory<TV, TE> dataflowTaskEdgeFactory;
  private DirectedDataflowTaskGraphAbstraction<TV, TE> directedDataflowTaskGraph = null;
  private Map<TE, DirectedDataflowTaskEdge> dataflowTaskEdgeMap;
  private IDataflowTaskEdgeSetFactory<TV, TE> dataflowTaskEdgeSetFactory;
  private DataflowTaskGraphUtils<TV> dataflowTaskGraphUtils = null;
  private Set<TE> taskEdgeSet = null;
  private Set<TV> taskVertexSet = null;

  public DataflowTaskGraphAbstraction(IDataflowTaskEdgeFactory<TV, TE> taskEdgeFactory) {

    if (taskEdgeFactory == null) {
      throw new NullPointerException();
    }
    this.dataflowTaskEdgeFactory = taskEdgeFactory;
    this.dataflowTaskEdgeMap = new LinkedHashMap<TE, DirectedDataflowTaskEdge>();
    this.dataflowTaskEdgeSetFactory = new TaskArrayListFactory<TV, TE>();
    this.dataflowTaskGraphUtils = new DataflowTaskGraphUtils<>();
    this.directedDataflowTaskGraph = createDirectedDataflowGraph();
  }

  public IDataflowTaskEdgeSetFactory<TV, TE> getDataflowTaskEdgeSetFactory() {
    return dataflowTaskEdgeSetFactory;
  }

  public void setDataflowTaskEdgeSetFactory(
      IDataflowTaskEdgeSetFactory<TV, TE> dataflowTaskEdgeSetFactory) {
    this.dataflowTaskEdgeSetFactory = dataflowTaskEdgeSetFactory;
  }

  @Override
  public IDataflowTaskEdgeFactory<TV, TE> getDataflowTaskEdgeFactory() {
    return dataflowTaskEdgeFactory;
  }

  public void setDataflowTaskEdgeFactory(IDataflowTaskEdgeFactory<TV, TE> dataflowTaskEdgeFactory) {
    this.dataflowTaskEdgeFactory = dataflowTaskEdgeFactory;
  }

  public boolean addTaskVertex(TV taskVertex) {
    if (taskVertex == null) {
      throw new NullPointerException();
    } else if (this.containsTaskVertex(taskVertex)) {
      return false;
    } else {
      this.directedDataflowTaskGraph.addTaskVertex(taskVertex);
      return true;
    }
  }

  @Override
  public TE addTaskEdge(TV sourceTaskVertex, TV targetTaskVertex) {

    validateTaskVertex(sourceTaskVertex);
    validateTaskVertex(targetTaskVertex);

    TE taskEdge = null;
    try {
      taskEdge = dataflowTaskEdgeFactory.createTaskEdge(sourceTaskVertex, targetTaskVertex);
    } catch (IllegalAccessException e) {
      e.printStackTrace();
    } catch (InstantiationException e) {
      e.printStackTrace();
    }

    if (containsTaskEdge(taskEdge)) {
      return null;
    } else {
      DirectedDataflowTaskEdge directedDataflowTaskEdge =
          createDirectedDataflowTaskEdge(taskEdge, sourceTaskVertex, targetTaskVertex);
      dataflowTaskEdgeMap.put(taskEdge, directedDataflowTaskEdge);
      directedDataflowTaskGraph.addTaskEdgeTVertices(taskEdge);
      return taskEdge;
    }
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

    DirectedDataflowTaskEdge directedDataflowTaskEdge =
        createDirectedDataflowTaskEdge(taskEdge, taskVertex1, taskVertex2);
    dataflowTaskEdgeMap.put(taskEdge, directedDataflowTaskEdge);
    directedDataflowTaskGraph.addTaskEdgeTVertices(taskEdge);

    return true;
  }

  private DirectedDataflowTaskEdge createDirectedDataflowTaskEdge(
      TE taskEdge, TV sourceTaskVertex, TV targetTaskVertex) {

    DirectedDataflowTaskEdge directedDataflowTaskEdge;

    if (taskEdge instanceof DirectedDataflowTaskEdge) {
      directedDataflowTaskEdge = (DirectedDataflowTaskEdge) taskEdge;
    } else {
      directedDataflowTaskEdge = new DirectedDataflowTaskEdge();
    }
    directedDataflowTaskEdge.sourceTaskVertex = sourceTaskVertex;
    directedDataflowTaskEdge.targetTaskVertex = targetTaskVertex;

    return directedDataflowTaskEdge;
  }


  @Override
  public TE getTaskEdge(TV sourceTaskVertex, TV targetTaskVertex) {
    return directedDataflowTaskGraph.getTaskEdge(sourceTaskVertex, targetTaskVertex);
  }

  public Set<TE> getAllTaskEdges(TV sourceTaskVertex, TV targetTaskVertex) {
    return directedDataflowTaskGraph.getAllTaskEdges(
        sourceTaskVertex, targetTaskVertex);
  }

  @Override
  public boolean containsTaskEdge(TE taskEdge) {
    return dataflowTaskEdgeMap.containsKey(taskEdge);
  }

  @Override
  public boolean containsTaskVertex(TV taskVertex) {
    boolean flag = directedDataflowTaskGraph.getTaskVertexSet().contains(taskVertex);
    return flag;
  }

  public Set<TE> incomingTaskEdgesOf(TV taskVertex) {
    return directedDataflowTaskGraph.incomingTaskEdgesOf(taskVertex);
  }

  public int outDegreeOf(TV taskVertex) {
    return directedDataflowTaskGraph.outDegreeOf(taskVertex);
  }

  public TE removeTaskEdge(TV sourceVertex, TV targetVertex) {
    TE taskEdge = getTaskEdge(sourceVertex, targetVertex);
    if (taskEdge != null) {
      directedDataflowTaskGraph.removeTaskEdgeTVertices(taskEdge);
      dataflowTaskEdgeMap.remove(taskEdge);
    }
    return taskEdge;
  }

  public boolean removeTaskEdge(TE taskEdge) {
    if (containsTaskEdge(taskEdge)) {
      directedDataflowTaskGraph.removeTaskEdgeTVertices(taskEdge);
      dataflowTaskEdgeMap.remove(taskEdge);
      return true;
    } else {
      return false;
    }
  }

  public boolean removeTaskVertex(TV taskVertex) {
    if (containsTaskVertex(taskVertex)) {
      Set<TE> touchingTaskEdgesList = taskEdgesOf(taskVertex);
      removeAllTaskEdges(new ArrayList<TE>(touchingTaskEdgesList));
      directedDataflowTaskGraph.getTaskVertexSet().remove(taskVertex);
      return true;
    } else {
      return false;
    }
  }

  @Override
  public Set<TV> getTaskVertexSet() {
    if (taskVertexSet == null) {
      taskVertexSet =
          Collections.unmodifiableSet(directedDataflowTaskGraph.getTaskVertexSet());
    }
    return taskVertexSet;
  }

  public Set<TE> taskEdgeSet() {
    if (taskEdgeSet == null) {
      taskEdgeSet =
          Collections.unmodifiableSet(dataflowTaskEdgeMap.keySet());
    }
    return taskEdgeSet;
  }

  public Set<TE> taskEdgesOf(TV taskVertex) {
    this.validateTaskVertex(taskVertex);
    return directedDataflowTaskGraph.taskEdgesOf(taskVertex);
  }

  public Set<TE> outgoingTaskEdgesOf(TV taskVertex) {
    this.validateTaskVertex(taskVertex);
    return directedDataflowTaskGraph.outgoingTaskEdgesOf(taskVertex);
  }

  public int inDegreeOf(TV taskVertex) {
    this.validateTaskVertex(taskVertex);
    return directedDataflowTaskGraph.inDegreeOf(taskVertex);
  }

  @Override
  public TV getTaskEdgeSource(TE taskEdge) {
    return dataflowTaskGraphUtils.DataflowTaskGraphCast(
        getDataflowTaskEdge(taskEdge).sourceTaskVertex,
        dataflowTaskGraphUtils);

  }

  @Override
  public TV getTaskEdgeTarget(TE taskEdge) {
    return dataflowTaskGraphUtils.DataflowTaskGraphCast(
        getDataflowTaskEdge(taskEdge).targetTaskVertex,
        dataflowTaskGraphUtils);
  }

  @Override
  public boolean containsTaskEdge(TV sourceTaskVertex,
                                  TV targetTaskVertex) {
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

  @Override
  public boolean removeAllTaskVertices(Collection<? extends TV>
                                           taskVertices) {
    boolean flag = false;
    for (TV taskVertex : taskVertices) {
      flag |= removeTaskVertex(taskVertex);
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

  public DirectedDataflowTaskEdge getDataflowTaskEdge(TE taskEdge) {
    if (taskEdge instanceof DirectedDataflowTaskEdge) {
      return (DirectedDataflowTaskEdge) taskEdge;
    }
    return dataflowTaskEdgeMap.get(taskEdge);
  }


  public DirectedDataflowTaskGraphAbstraction<TV, TE> createDirectedDataflowGraph() {
    if (this instanceof DataflowTaskGraph<?, ?>) {
      return createDirectedDataflowTaskGraph();
    } else {
      throw new IllegalArgumentException("Dataflow Task Graph must be Directed");
    }
  }

  public DirectedDataflowTaskGraph createDirectedDataflowTaskGraph() {
    return new DirectedDataflowTaskGraph();
  }

  public class DirectedDataflowTaskGraph extends
      DirectedDataflowTaskGraphAbstraction<TV, TE> {

    protected Map<TV, DirectedDataflowTaskEdgeCreator<TV, TE>> taskVertexMap;

    public DirectedDataflowTaskGraph() {
      this(new LinkedHashMap<TV, DirectedDataflowTaskEdgeCreator<TV, TE>>());
    }

    public DirectedDataflowTaskGraph(Map<TV,
        DirectedDataflowTaskEdgeCreator<TV, TE>> vertexTaskMap) {
      this.taskVertexMap = vertexTaskMap;
    }

    @Override
    public void addTaskVertex(TV taskVertex) {
      taskVertexMap.put(taskVertex, null);
    }

    @Override
    public Set<TV> getTaskVertexSet() {
      return taskVertexMap.keySet();
    }

    @Override
    public Set<TE> getAllTaskEdges(TV sourceTaskVertex, TV targetTaskVertex) {
      Set<TE> taskEdges = null;

      if (containsTaskVertex(sourceTaskVertex)
          && containsTaskVertex(targetTaskVertex)) {

        taskEdges = new TaskArraySet<TE>();

        DirectedDataflowTaskEdgeCreator<TV, TE> directedDataflowTaskEdgeCreator =
            getTaskEdgeContainer(sourceTaskVertex);
        Iterator<TE> iterator = directedDataflowTaskEdgeCreator.outgoingTaskEdge.iterator();

        while (iterator.hasNext()) {
          TE taskEdge = iterator.next();
          if (getTaskEdgeTarget(taskEdge).equals(targetTaskVertex)) {
            taskEdges.add(taskEdge);
          }
        }
      }
      return taskEdges;
    }

    @Override
    public TE getTaskEdge(TV sourceTaskVertex, TV targetTaskVertex) {
      return null;
    }

    @Override
    public Set<TE> taskEdgesOf(TV taskVertex) {
      TaskArraySet<TE> inOutTaskEdgeSet =
          new TaskArraySet<TE>(getTaskEdgeContainer(taskVertex).incomingTaskEdge);
      inOutTaskEdgeSet.addAll(getTaskEdgeContainer(taskVertex).outgoingTaskEdge);
      return Collections.unmodifiableSet(inOutTaskEdgeSet);
    }

    @Override
    public int inDegreeOf(TV taskVertex) {
      return getTaskEdgeContainer(taskVertex).incomingTaskEdge.size();
    }

    @Override
    public int outDegreeOf(TV taskVertex) {
      return getTaskEdgeContainer(taskVertex).outgoingTaskEdge.size();
    }

    @Override
    public Set<TE> incomingTaskEdgesOf(TV taskVertex) {
      return getTaskEdgeContainer(taskVertex).getImmutableIncomingTaskEdges();
    }

    @Override
    public Set<TE> outgoingTaskEdgesOf(TV taskVertex) {
      return getTaskEdgeContainer(taskVertex).getImmutableOutgoingTaskEdges();
    }

    @Override
    public void addTaskEdgeTVertices(TE taskEdge) {

      TV source = getTaskEdgeSource(taskEdge);
      TV target = getTaskEdgeTarget(taskEdge);

      getTaskEdgeContainer(source).addOutgoingTaskEdge(taskEdge);
      getTaskEdgeContainer(target).addIncomingTaskEdge(taskEdge);
    }

    @Override
    public void removeTaskEdgeTVertices(TE taskEdge) {

      TV source = getTaskEdgeSource(taskEdge);
      TV target = getTaskEdgeSource(taskEdge);

      getTaskEdgeContainer(source).removeOutgoingTaskEdge(taskEdge);
      getTaskEdgeContainer(target).removeIncomingTaskEdge(taskEdge);
    }

    private DirectedDataflowTaskEdgeCreator<TV, TE> getTaskEdgeContainer(
        TV taskVertex) {
      validateTaskVertex(taskVertex);
      DirectedDataflowTaskEdgeCreator<TV, TE> ec = taskVertexMap.get(taskVertex);
      if (ec == null) {
        try {
          ec = new DirectedDataflowTaskEdgeCreator<>(dataflowTaskEdgeSetFactory, taskVertex);
        } catch (InstantiationException e) {
          e.printStackTrace();
        } catch (IllegalAccessException e) {
          e.printStackTrace();
        }
        taskVertexMap.put(taskVertex, ec);
      }
      return ec;
    }
  }

  private class TaskArrayListFactory<TV, TE> implements
      IDataflowTaskEdgeSetFactory<TV, TE> {
    public Set<TE> createTaskEdgeSet(TV taskVertex) {
      return new TaskArraySet<TE>(1);
    }
  }
}
