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

import java.util.logging.Logger;

import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.task.api.Task;

/**
 * This is the main class for creating the dataflow task graph.
 */
public class DataflowTaskGraphGenerator {

  private static final Logger LOGGER = Logger.getLogger(
      DataflowTaskGraphGenerator.class.getName());

  private IDataflowTaskGraph<TaskMapper, CManager> dataflowTaskGraph =
      new DataflowTaskGraph<>(CManager.class);

  private IDataflowTaskGraph<TaskMapper, DataflowTaskEdge> taskGraph =
      new DataflowTaskGraph<>(DataflowTaskEdge.class);

  private IDataflowTaskGraph<Task, DataFlowOperation> dataflowGraph =
      new DataflowTaskGraph<>(DataFlowOperation.class);


  public IDataflowTaskGraph<TaskMapper, DataflowTaskEdge> getTaskGraph() {
    return taskGraph;
  }

  public void setTaskGraph(IDataflowTaskGraph<TaskMapper,
      DataflowTaskEdge> taskGraph) {
    this.taskGraph = taskGraph;
  }

  public IDataflowTaskGraph<TaskMapper, CManager> getDataflowTaskGraph() {
    return dataflowTaskGraph;
  }

  public void setDataflowTaskGraph(IDataflowTaskGraph<TaskMapper,
      CManager> dataflowTaskGraph) {
    this.dataflowTaskGraph = dataflowTaskGraph;
  }

  public IDataflowTaskGraph<Task, DataFlowOperation> getDataflowGraph() {
    return dataflowGraph;
  }

  public void setDataflowGraph(IDataflowTaskGraph<Task, DataFlowOperation> dataflowGraph) {
    this.dataflowGraph = dataflowGraph;
  }

  public DataflowTaskGraphGenerator generateDataflowGraph(Task sourceTask,
                                                          Task sinkTask,
                                                          DataFlowOperation... dataFlowOperation) {
    try {
      this.dataflowGraph.addTaskVertex(sourceTask);
      this.dataflowGraph.addTaskVertex(sinkTask);
      for (DataFlowOperation dataflowOperation1 : dataFlowOperation) {
        this.dataflowGraph.addTaskEdge(sourceTask, sinkTask, dataFlowOperation[0]);
      }
    } catch (Exception iae) {
      iae.printStackTrace();
    }
    LOGGER.info("Generated Dataflow Task Graph Is:" + taskGraph);
    LOGGER.info("Generated Dataflow Task Graph with Vertices is:"
        + taskGraph.getTaskVertexSet().size());
    return this;
  }

  public DataflowTaskGraphGenerator generateTaskGraph(TaskMapper taskMapperTask1,
                                                      TaskMapper... taskMapperTask2) {
    try {
      this.taskGraph.addTaskVertex(taskMapperTask1);
      for (TaskMapper taskMapperTask : taskMapperTask2) {
        this.taskGraph.addTaskEdge(taskMapperTask, taskMapperTask1);
      }
    } catch (IllegalArgumentException iae) {
      iae.printStackTrace();
    }

    LOGGER.info("Generated Dataflow Task Graph Is:" + taskGraph);
    LOGGER.info("Generated Dataflow Task Graph with Vertices is:"
        + taskGraph.getTaskVertexSet().size());
    return this;
  }

  public DataflowTaskGraphGenerator generateDataflowTaskGraph(TaskMapper taskMapperTask1,
                                                              TaskMapper taskMapperTask2,
                                                              CManager... cManagerTask) {
    try {
      this.dataflowTaskGraph.addTaskVertex(taskMapperTask1);
      this.dataflowTaskGraph.addTaskVertex(taskMapperTask2);
      for (CManager cManagerTask1 : cManagerTask) {
        this.dataflowTaskGraph.addTaskEdge(taskMapperTask1, taskMapperTask2, cManagerTask[0]);
      }
    } catch (Exception iae) {
      iae.printStackTrace();
    }
    LOGGER.info("Generated Dataflow Task Graph Is:" + taskGraph);
    LOGGER.info("Generated Dataflow Task Graph with Vertices is:"
        + taskGraph.getTaskVertexSet().size());
    return this;
  }
}



