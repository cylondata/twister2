package edu.iu.dsc.tws.task.taskgraphbuilder;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class DataflowTaskGraph<TV, TE> extends AbstractDataflowTaskGraphImpl<TV, TE>
    implements IDataflowTaskGraph<TV, TE> {

  private static final long serialVersionUID = 2233233333444449278L;

  public Map<TV, DataflowEdge<TV, TE>> vertexMapDataflow = new LinkedHashMap<>();

  public DataflowTaskGraph(Class<? extends TE> taskEdgeClass) {
    this(new DataflowTaskEdgeFactory<TV, TE>(taskEdgeClass));
  }

  public DataflowTaskGraph(IDataflowTaskEdgeFactory<TV, TE> taskEdgeFactory) {
    super(taskEdgeFactory);
  }


  public boolean addTaskVertex(TV taskVertex) {
    vertexMapDataflow.put(taskVertex, null);
    System.out.println("TaskVertex Map Details are:" + vertexMapDataflow);
    return true;
  }

  @Override
  public Set<TV> getTaskVertexSet() {
    return vertexMapDataflow.keySet();
  }

  @Override
  public Set<TE> taskEdgeSet() {
    return null;
  }

  @Override
  public TE removeTaskEdge(TV sourceTaskVertex, TV targetTaskVertex) {
    return null;
  }

  @Override
  public boolean removeTaskVertex(TV taskVertex) {
    return false;
  }

  @Override
  public int inDegreeOf(TV taskVertex) {
    return 0;
  }

  @Override
  public Set<TE> incomingTaskEdgesOf(TV taskVertex) {
    return null;
  }

  @Override
  public int outDegreeOf(TV taskVertex) {
    return 0;
  }

  @Override
  public Set<TE> outgoingTaskEdgesOf(TV taskVertex) {
    return null;
  }

  private class DataflowEdge<TV, TE> {

  }
}

