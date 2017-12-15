package edu.iu.dsc.tws.task.taskgraphbuilder;

import java.util.HashSet;
import java.util.Set;

/**
 * This is the main class for creating the dataflow task graph.
 */
public class DataflowTaskGraphGenerator {

  public static int totalNumberOfTasks = 0;

  private IDataflowTaskGraph<Mapper, CManager> dataflowTaskGraph =
      new DataflowTaskGraph<Mapper, CManager>(CManager.class);

  private IDataflowTaskGraph<Mapper, DataflowTaskEdge> taskGraph =
      new DataflowTaskGraph<>(DataflowTaskEdge.class);

  private Set<Mapper> runningTasks = new HashSet<>();

  public IDataflowTaskGraph<Mapper, CManager> getDataflowTaskGraph() {
    return dataflowTaskGraph;
  }

  public void setDataflowTaskGraph(IDataflowTaskGraph<Mapper, CManager>
                                       dataflowTaskGraph) {
    this.dataflowTaskGraph = dataflowTaskGraph;
  }

  public IDataflowTaskGraph<Mapper, DataflowTaskEdge> getTaskGraph() {
    return taskGraph;
  }

  public void setTaskGraph(IDataflowTaskGraph<Mapper,
      DataflowTaskEdge> taskGraph) {
    this.taskGraph = taskGraph;
  }

  public DataflowTaskGraphGenerator generateTaskGraph(Mapper mapperTask1,
                                                      Mapper... mapperTask2) {
    try {
      this.taskGraph.addTaskVertex(mapperTask1);
      for (Mapper mapperTask : mapperTask2) {
        this.taskGraph.addTaskEdge(mapperTask, mapperTask1);
      }
    } catch (IllegalArgumentException iae) {
      iae.printStackTrace();
    }
    System.out.println("Generated Task Graph Is:" + taskGraph);
    System.out.println("Generated Task Graph with Vertices is:"
        + taskGraph.getTaskVertexSet().size());
    ++totalNumberOfTasks;
    return this;
  }

  public DataflowTaskGraphGenerator generateDataflowGraph(Mapper mapperTask1,
                                                          Mapper mapperTask2,
                                                          CManager... cManagerTask) {
    try {
      this.dataflowTaskGraph.addTaskVertex(mapperTask1);
      this.dataflowTaskGraph.addTaskVertex(mapperTask2);
      for (CManager cManagerTask1 : cManagerTask) {
        this.dataflowTaskGraph.addTaskEdge(mapperTask1, mapperTask2, cManagerTask[0]);
      }
    } catch (Exception iae) {
      iae.printStackTrace();
    }

    System.out.println("Generated Task Graph with Vertices is:"
        + dataflowTaskGraph.getTaskVertexSet().size());
    System.out.println("Task Graph is:" + dataflowTaskGraph);

    ++totalNumberOfTasks;
    return this;
  }
}


