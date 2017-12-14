package edu.iu.dsc.tws.task.taskgraphbuilder;

public class DataflowTaskEdgeFactory<TV, TE> implements
    IDataflowTaskEdgeFactory<TV, TE> {

  public Class<? extends TE> taskEdgeClass;

  public DataflowTaskEdgeFactory(Class<? extends TE> taskEdgeClass) {
    this.taskEdgeClass = taskEdgeClass;
  }

  @Override
  public TE createTaskEdge(TV sourceTaskVertex, TV targetTaskVertex)
      throws IllegalAccessException {
    System.out.println("Source Task Vertex is:" + sourceTaskVertex);
    try {
      return taskEdgeClass.newInstance();
    } catch (InstantiationException e) {
      throw new RuntimeException("instance creation failed", e);
    }
  }
}

