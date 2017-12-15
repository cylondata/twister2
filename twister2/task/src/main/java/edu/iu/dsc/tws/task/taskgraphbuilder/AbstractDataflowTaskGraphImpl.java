package edu.iu.dsc.tws.task.taskgraphbuilder;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/**
 * It is an abstract implementation of the task graph class which is mainly responsible for creating
 * the task edge factory, task vertex and task edge map.
 */
public abstract class AbstractDataflowTaskGraphImpl<TV, TE>
    extends AbstractDataflowTaskGraph<TV, TE>
    implements ITaskGraph<TV, TE> {

  private IDataflowTaskEdgeFactory<TV, TE> dataflowTaskEdgeFactory;
  private DirectedDataflowGraph<TV, TE> directedDataflowTaskGraph = null;
  private Map<TE, DirectedDataflowTaskEdge> dataflowTaskEdgeMap;
  private IDataflowTaskEdgeSetFactory<TV, TE> dataflowTaskEdgeSetFactory;
  private DataflowTaskGraphUtils<TV> dataflowTaskGraphUtils = null;
  private Set<TE> taskEdgeSet = null;
  private Set<TV> taskVertexSet = null;

  public AbstractDataflowTaskGraphImpl(IDataflowTaskEdgeFactory<TV, TE> taskEdgeFactory) {
    if (taskEdgeFactory == null) {
      throw new NullPointerException();
    }
    this.dataflowTaskEdgeFactory = taskEdgeFactory;
    this.dataflowTaskEdgeMap = new LinkedHashMap<TE, DirectedDataflowTaskEdge>();
    this.dataflowTaskEdgeSetFactory = new ArrayListFactory<TV, TE>();
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

    assertTaskVertexExist(sourceTaskVertex);
    assertTaskVertexExist(targetTaskVertex);

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
      directedDataflowTaskGraph.addTaskEdgeToTouchingVertices(taskEdge);
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

    assertTaskVertexExist(taskVertex1);
    assertTaskVertexExist(taskVertex2);

    DirectedDataflowTaskEdge directedDataflowTaskEdge =
        createDirectedDataflowTaskEdge(taskEdge, taskVertex1, taskVertex2);
    dataflowTaskEdgeMap.put(taskEdge, directedDataflowTaskEdge);
    directedDataflowTaskGraph.addTaskEdgeToTouchingVertices(taskEdge);
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

  public DirectedDataflowGraph<TV, TE> createDirectedDataflowGraph() {
    if (this instanceof DataflowTaskGraph<?, ?>) {
      return createDirectedDataflowTaskGraph();
    } else {
      throw new IllegalArgumentException("Dataflow Task Graph must be Directed");
    }
  }

  @Override
  public TE getTaskEdge(TV sourceTaskVertex, TV targetTaskVertex) {
    return directedDataflowTaskGraph.getTaskEdge(sourceTaskVertex, targetTaskVertex);
  }

  public Set<TE> getAllTaskEdges(TV sourceTaskVertex, TV targetTaskVertex) {
    return directedDataflowTaskGraph.getAllTaskEdges(sourceTaskVertex, targetTaskVertex);
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

  public int degreeOf(TV taskVertex) {
    return directedDataflowTaskGraph.degreeOf(taskVertex);
  }

  public Set<TE> incomingTaskEdgesOf(TV taskVertex) {
    return directedDataflowTaskGraph.incomingTaskEdgesOf(taskVertex);
  }

  public int outDegreeOf(TV taskVertex) {
    return directedDataflowTaskGraph.outDegreeOf(taskVertex);
  }

  public Set<TE> outgoingEdgesOf(TV taskVertex) {
    return directedDataflowTaskGraph.outgoingTaskEdgesOf(taskVertex);
  }

  public TE removeTaskEdge(TV sourceVertex, TV targetVertex) {
    TE taskEdge = getTaskEdge(sourceVertex, targetVertex);
    if (taskEdge != null) {
      directedDataflowTaskGraph.removeTaskEdgeFromTouchingVertices(taskEdge);
      dataflowTaskEdgeMap.remove(taskEdge);
    }
    return taskEdge;
  }

  public boolean removeTaskEdge(TE taskEdge) {
    if (containsTaskEdge(taskEdge)) {
      directedDataflowTaskGraph.removeTaskEdgeFromTouchingVertices(taskEdge);
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

  public Set<TE> taskEdgeSet() {
    if (taskEdgeSet == null) {
      taskEdgeSet =
          Collections.unmodifiableSet(dataflowTaskEdgeMap.keySet());
    }
    return taskEdgeSet;
  }

  public Set<TE> taskEdgesOf(TV taskVertex) {
    this.assertTaskVertexExist(taskVertex);
    return directedDataflowTaskGraph.taskEdgesOf(taskVertex);
  }

  public Set<TE> outgoingTaskEdgesOf(TV taskVertex) {
    this.assertTaskVertexExist(taskVertex);
    return directedDataflowTaskGraph.outgoingTaskEdgesOf(taskVertex);
  }

  public int inDegreeOf(TV taskVertex) {
    this.assertTaskVertexExist(taskVertex);
    return directedDataflowTaskGraph.inDegreeOf(taskVertex);
  }

  public DirectedDataflowTaskGraph createDirectedDataflowTaskGraph() {
    return new DirectedDataflowTaskGraph();
  }

  @Override
  public TV getTaskEdgeSource(TE taskEdge) {
    return dataflowTaskGraphUtils.uncheckedCast(
        getDataflowTaskEdge(taskEdge).sourceTaskVertex,
        dataflowTaskGraphUtils);

  }

  @Override
  public TV getTaskEdgeTarget(TE taskEdge) {
    return dataflowTaskGraphUtils.uncheckedCast(
        getDataflowTaskEdge(taskEdge).targetTaskVertex,
        dataflowTaskGraphUtils);
  }

  public DirectedDataflowTaskEdge getDataflowTaskEdge(TE taskEdge) {
    if (taskEdge instanceof DirectedDataflowTaskEdge) {
      return (DirectedDataflowTaskEdge) taskEdge;
    }
    return dataflowTaskEdgeMap.get(taskEdge);
  }

  @Override
  public Set<TV> getTaskVertexSet() {
    if (taskVertexSet == null) {
      taskVertexSet =
          Collections.unmodifiableSet(directedDataflowTaskGraph.getTaskVertexSet());
    }
    return taskVertexSet;
  }

  public class DirectedDataflowTaskGraph extends DirectedDataflowGraph<TV, TE> {

    protected Map<TV, DirectedDataflowTaskEdgeContainer<TV, TE>> taskVertexMap;

    public DirectedDataflowTaskGraph() {
      this(new LinkedHashMap<TV, DirectedDataflowTaskEdgeContainer<TV, TE>>());
    }

    public DirectedDataflowTaskGraph(Map<TV,
        DirectedDataflowTaskEdgeContainer<TV, TE>> vertexTaskMap) {
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

        taskEdges = new ArraySet<TE>();

        DirectedDataflowTaskEdgeContainer<TV, TE> directedDataflowTaskEdgeContainer =
            getTaskEdgeContainer(sourceTaskVertex);

        Iterator<TE> iterator = directedDataflowTaskEdgeContainer.outgoingTaskEdge.iterator();

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
    public void addTaskEdgeToTouchingVertices(TE taskEdge) {

      TV source = getTaskEdgeSource(taskEdge);
      TV target = getTaskEdgeTarget(taskEdge);

      getTaskEdgeContainer(source).addOutgoingTaskEdge(taskEdge);
      getTaskEdgeContainer(target).addIncomingTaskEdge(taskEdge);
    }

    @Override
    public int degreeOf(TV taskVertex) {
      return 0;
    }

    @Override
    public Set<TE> taskEdgesOf(TV taskVertex) {
      ArraySet<TE> inAndOut =
          new ArraySet<TE>(getTaskEdgeContainer(taskVertex).incomingTaskEdge);
      inAndOut.addAll(getTaskEdgeContainer(taskVertex).outgoingTaskEdge);

      return Collections.unmodifiableSet(inAndOut);
    }

    @Override
    public int inDegreeOf(TV taskVertex) {
      return getTaskEdgeContainer(taskVertex).incomingTaskEdge.size();
    }

    @Override
    public Set<TE> incomingTaskEdgesOf(TV taskVertex) {
      return getTaskEdgeContainer(taskVertex).getUnmodifiableIncomingTaskEdges();
    }

    @Override
    public int outDegreeOf(TV taskVertex) {
      return getTaskEdgeContainer(taskVertex).outgoingTaskEdge.size();
    }

    @Override
    public Set<TE> outgoingTaskEdgesOf(TV taskVertex) {
      return getTaskEdgeContainer(taskVertex).getUnmodifiableOutgoingTaskEdges();
    }

    @Override
    public void removeTaskEdgeFromTouchingVertices(TE taskEdge) {

      TV source = getTaskEdgeSource(taskEdge);
      TV target = getTaskEdgeSource(taskEdge);

      getTaskEdgeContainer(source).removeOutgoingTaskEdge(taskEdge);
      getTaskEdgeContainer(target).removeIncomingTaskEdge(taskEdge);
    }

    private DirectedDataflowTaskEdgeContainer<TV, TE> getTaskEdgeContainer(
        TV taskVertex) {

      assertTaskVertexExist(taskVertex);
      DirectedDataflowTaskEdgeContainer<TV, TE> ec = taskVertexMap.get(taskVertex);
      if (ec == null) {
        try {
          ec = new DirectedDataflowTaskEdgeContainer<TV, TE>(
              dataflowTaskEdgeSetFactory, taskVertex);
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

  private class ArrayListFactory<TV, TE> implements IDataflowTaskEdgeSetFactory<TV, TE> {

    public Set<TE> createTaskEdgeSet(TV taskVertex) {
      return new ArraySet<TE>(1);
    }
  }
}
