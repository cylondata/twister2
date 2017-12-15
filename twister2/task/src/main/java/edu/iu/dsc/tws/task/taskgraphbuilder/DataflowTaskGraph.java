package edu.iu.dsc.tws.task.taskgraphbuilder;

public class DataflowTaskGraph<TV, TE> extends AbstractDataflowTaskGraphImpl<TV, TE>
    implements IDataflowTaskGraph<TV, TE> {

  public DataflowTaskGraph(Class<? extends TE> taskEdgeClass) {
    this(new DataflowTaskEdgeFactory<TV, TE>(taskEdgeClass));
  }

  public DataflowTaskGraph(IDataflowTaskEdgeFactory<TV, TE> taskEdgeFactory) {
    super(taskEdgeFactory);
  }
}


