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

import edu.iu.dsc.tws.task.taskgraphfluentapi.ITaskInfo;

/**
 * This is the main class for creating the dataflow task graph.
 */
public class DataflowTaskGraphGenerator implements IDataflowTaskGraphGenerator {

  private static final Logger LOGGER = Logger.getLogger(
      DataflowTaskGraphGenerator.class.getName());

  private IDataflowTaskGraph<TaskMapper, CManager> dataflowTaskGraph =
      new DataflowTaskGraph<>(CManager.class);

  private IDataflowTaskGraph<TaskMapper, DataflowTaskEdge> taskGraph =
      new DataflowTaskGraph<>(DataflowTaskEdge.class);

  private IDataflowTaskGraph<Task, DataFlowOperation> dataflowGraph =
      new DataflowTaskGraph<>(DataFlowOperation.class);

  /**
   * Newly added code for defining the task edges as dataflow operations namely
   * Map, Reduce, Shuffle, and others.
   */
  private IDataflowTaskGraph<Task, DataflowOperation> taskgraph =
      new DataflowTaskGraph<>(DataflowOperation.class);

  private IDataflowTaskGraph<TaskGraphMapper, DataflowOperation> tGraph =
      new DataflowTaskGraph<>(DataflowOperation.class);

  private IDataflowTaskGraph<ITaskInfo, DataflowOperation> iTaskGraph =
      new DataflowTaskGraph<>(DataflowOperation.class);

  public IDataflowTaskGraph<ITaskInfo, DataflowOperation> getITaskGraph() {
    return iTaskGraph;
  }

  public void setITaskGraph(IDataflowTaskGraph<ITaskInfo, DataflowOperation> iTaskgraph) {
    this.iTaskGraph = iTaskgraph;
  }

  /**
   * This method is responsible for creating the dataflow task graph from the receiving
   * task vertices and task eges.
   */
  public DataflowTaskGraphGenerator generateITaskGraph(
      DataflowOperation dataflowOperation,
      ITaskInfo taskVertex, ITaskInfo... taskEdge) {
    try {
      this.iTaskGraph.addTaskVertex(taskVertex);
      if (taskEdge.length >= 1) {
        this.iTaskGraph.addTaskVertex(taskEdge[0]);
        this.iTaskGraph.addTaskEdge(taskVertex, taskEdge[0], dataflowOperation);
      }

            /*for (ITaskInfo mapperTask : taskEdge) {
                this.iTaskGraph.addTaskEdge (mapperTask, taskVertex);
            }*/
    } catch (IllegalArgumentException iae) {
      iae.printStackTrace();
    } /*catch (IllegalAccessException e) {
            e.printStackTrace ();
        } catch (InstantiationException e) {
            e.printStackTrace ();
        }*/
    System.out.println("Constructed Task Graph is:" + iTaskGraph.getTaskVertexSet().size());
    return this;
  }

  public IDataflowTaskGraph<Task, DataflowOperation> getTaskgraph() {
    return taskgraph;
  }

  public void setTaskgraph(IDataflowTaskGraph<Task,
      DataflowOperation> taskgraph) {
    this.taskgraph = taskgraph;
  }

  public IDataflowTaskGraph<TaskGraphMapper, DataflowOperation> getTGraph() {
    return tGraph;
  }

  public void setTGraph(IDataflowTaskGraph<TaskGraphMapper, DataflowOperation> tgraph) {
    this.tGraph = tgraph;
  }

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

  public DataflowTaskGraphGenerator generateTGraph(TaskGraphMapper sourceTask,
                                                   TaskGraphMapper sinkTask,
                                                   DataflowOperation... dataflowOperation) {
    try {
      this.tGraph.addTaskVertex(sourceTask);
      this.tGraph.addTaskVertex(sinkTask);
      this.tGraph.addTaskEdge(sourceTask, sinkTask, dataflowOperation[0]);
    } catch (IllegalArgumentException iae) {
      iae.printStackTrace();
    }
    return this;
  }

  public DataflowTaskGraphGenerator generateTGraph(TaskGraphMapper taskGraphMapper1,
                                                   TaskGraphMapper... taskGraphMappers) {
    try {
      this.tGraph.addTaskVertex(taskGraphMapper1);
      for (TaskGraphMapper mapperTask : taskGraphMappers) {
        this.tGraph.addTaskEdge(mapperTask, taskGraphMapper1);
      }
    } catch (IllegalArgumentException iae) {
      iae.printStackTrace();
    }
    return this;
  }

  public DataflowTaskGraphGenerator generateDataflowGraph(Task sourceTask,
                                                          Task sinkTask,
                                                          DataFlowOperation... dataFlowOperation) {
    try {
      this.dataflowGraph.addTaskVertex(sourceTask);
      this.dataflowGraph.addTaskVertex(sinkTask);
      for (DataFlowOperation dataflowOperation1 : dataFlowOperation) {
        this.dataflowGraph.addTaskEdge(
            sourceTask, sinkTask, dataFlowOperation[0]);
      }
    } catch (IllegalArgumentException iae) {
      iae.printStackTrace();
    }
    LOGGER.info("Generated Dataflow Task Graph Is:" + taskGraph);
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
    return this;
  }

  public DataflowTaskGraphGenerator generateDataflowTaskGraph(TaskMapper taskMapperTask1,
                                                              TaskMapper taskMapperTask2,
                                                              CManager... cManagerTask) {
    try {
      this.dataflowTaskGraph.addTaskVertex(taskMapperTask1);
      this.dataflowTaskGraph.addTaskVertex(taskMapperTask2);
      for (CManager cManagerTask1 : cManagerTask) {
        this.dataflowTaskGraph.addTaskEdge(
            taskMapperTask1, taskMapperTask2, cManagerTask[0]);
      }
    } catch (IllegalArgumentException iae) {
      iae.printStackTrace();
    }
    LOGGER.info("Generated Dataflow Task Graph Is:" + taskGraph);
    return this;
  }

  public DataflowTaskGraphGenerator generateTaskGraph(Task task1,
                                                      Task task2,
                                                      DataflowOperation... dataflowOperation) {
    LOGGER.info("**** Task Graph Generation Initiated ****");
    try {
      this.taskgraph.addTaskVertex(task1);
      this.taskgraph.addTaskVertex(task2);
      for (DataflowOperation dataFlowOperation1 : dataflowOperation) {
        this.taskgraph.addTaskEdge(task1, task2, dataflowOperation[0]);
      }
    } catch (IllegalArgumentException iae) {
      iae.printStackTrace();
    }
    LOGGER.info("Generated Dataflow Task Graph Is:" + taskGraph);
    return this;
  }

  public void removeTaskVertex(TaskGraphMapper mapperTask) {
    LOGGER.info("Mapper task done to be removed:" + mapperTask);
    this.tGraph.removeTaskVertex(mapperTask);
    LOGGER.info("Now the task graph is:" + this.dataflowTaskGraph);
  }

  public void removeTaskVertex(TaskMapper mapperTask) {
    LOGGER.info("Mapper task done to be removed:" + mapperTask);
    this.dataflowTaskGraph.removeTaskVertex(mapperTask);
    LOGGER.info("Now the task graph is:" + this.dataflowTaskGraph);
  }

  public void removeTaskVertex(Task mapperTask) {
    LOGGER.info("Mapper task done to be removed:" + mapperTask);
    this.dataflowGraph.removeTaskVertex(mapperTask);
    LOGGER.info("Now the task graph is:" + this.dataflowTaskGraph);
  }

  /**
   * It would be implemented later for the required use cases.
   */
  @Override
  public DataflowTaskGraphGenerator generateTaskGraph(
      Task sourceTask, Task... sinkTask) {
    return this;
  }

  /**
   * It would be implemented later for the required use cases.
   */
  @Override
  public DataflowTaskGraphGenerator generateDataflowTaskGraph(
      Task sourceTask, Task sinkTask, CManager... cManagerTask) {
    return this;
  }
}
