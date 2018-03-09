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
package edu.iu.dsc.tws.task.taskgraphfluentapi;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import java.util.logging.Logger;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;

import edu.iu.dsc.tws.task.taskgraphbuilder.DataflowOperation;
import edu.iu.dsc.tws.task.taskgraphbuilder.DataflowTaskGraph;
import edu.iu.dsc.tws.task.taskgraphbuilder.DataflowTaskGraphGenerator;

import edu.iu.dsc.tws.task.taskgraphbuilder.IDataflowTaskGraph;

/**
 * This class is mainly responsible for handling the user requests
 * to connect the task objects, generate the task graph, and submit
 * the task graph for the execution.
 */
public class TaskGraphGenerationHandler implements ITaskGraphGenerate {

  private static final Logger LOGGER = Logger.getLogger(
      TaskGraphGenerationHandler.class.getName());

  protected TaskInfo taskInfo = null; // new TaskInfo ();
  private DataflowTaskGraphGenerator dataflowTaskGraphGenerator = new DataflowTaskGraphGenerator();
  private List<ITaskInfo> taskInfoList = new ArrayList<>();
  private List<ITaskInfo> taskSelectedList = new ArrayList<>();
  private Multimap<Integer, Integer> taskConnectedList = ArrayListMultimap.create();
  private Multimap<ITaskInfo, ITaskInfo> taskInfoConnectedList = ArrayListMultimap.create();
  private Multimap<ITaskInfo, TaskInfo> taskgraphMap = ArrayListMultimap.create();
  private IDataflowTaskGraph<ITaskInfo, DataflowOperation> iTaskGraph =
      new DataflowTaskGraph<>(DataflowOperation.class);

  /**
   * This is the main constructor to construct the task objects
   * and their taskgraph.
   */
  public TaskGraphGenerationHandler() {
    /**
     * This tasks are sample task for generating the task list/graph. It will be removed
     * in later point.
     */
    ITaskInfo task1 = new ITaskInfo() {
      @Override
      public ITaskInfo taskName() {
        return this;
      }

      @Override
      public int taskId() {
        return 1;
      }
    };
    ITaskInfo task2 = new ITaskInfo() {
      @Override
      public ITaskInfo taskName() {
        return this;
      }

      @Override
      public int taskId() {
        return 1;
      }
    };
    ITaskInfo task3 = new ITaskInfo() {
      @Override
      public ITaskInfo taskName() {
        return this;
      }

      @Override
      public int taskId() {
        return 1;
      }
    };
    ITaskInfo task4 = new ITaskInfo() {
      @Override
      public ITaskInfo taskName() {
        return this;
      }

      @Override
      public int taskId() {
        return 1;
      }
    };
    taskInfoList.add(task1);
    taskInfoList.add(task2);
    taskInfoList.add(task3);
    taskInfoList.add(task4);
    LOGGER.info("Task Info List Values:" + taskInfoList);
  }

  public void showTaskGraph() {
    for (ITaskInfo taskInfoValue : taskInfoList) {
      taskInfoValue.taskName();
      taskInfoValue.taskId();
    }
  }

  public ITaskGraphGenerate generateTaskVertex(int taskIndex) {
    ITaskInfo taskInfoValue = taskInfoList.get(taskIndex);
    taskSelectedList.add(taskInfoValue);
    taskInfoValue.taskName();
    return this;
  }

  @Override
  public ITaskGraphGenerate connectTaskVertex_Edge(int taskVertexId,
                                                   int... taskEdgeId) {
    for (int i = 0; i < taskEdgeId.length; i++) {
      taskConnectedList.put(taskVertexId, taskEdgeId[i]);
    }
    return this;
  }

  @Override
  public ITaskGraphGenerate connectTaskVertex_Edge(ITaskInfo taskVertex,
                                                   ITaskInfo... taskEdge) {
    if (taskEdge.length != -1) {
      for (int i = 0; i < taskEdge.length; i++) {
        taskInfoConnectedList.put(taskVertex, taskEdge[i]);
      }
    }
    return this;
  }

  /**
   * This method is responsible for connecting the task vertices and the task
   * edge using the dataflow operation.
   */
  @Override
  public ITaskGraphGenerate connectTaskVertex_Edge(DataflowOperation dataflowOperation,
                                                   ITaskInfo sourceTask,
                                                   ITaskInfo... targetTask) {
    taskInfo = new TaskInfo();
    taskInfo.setSourceTask(sourceTask);
    taskInfo.setTargetTask(targetTask);
    taskInfo.setDataflowOperation(dataflowOperation);
    taskgraphMap.put(sourceTask, taskInfo);
    return this;
  }

  /**
   * It call the dataflow task graph generation method in Dataflow Task Graph
   * Generator class.
   */
  @Override
  public ITaskGraphGenerate build() {

    LOGGER.info("Task Graph Map:" + taskgraphMap.size() + "\t" + taskgraphMap);

    Set<ITaskInfo> keySet = taskgraphMap.keySet();
    Iterator<ITaskInfo> iterator = keySet.iterator();

    try {
      while (iterator.hasNext()) {
        ITaskInfo iTaskInfo = iterator.next();
        @SuppressWarnings("unchecked")
        List<ITaskInfo> value = (List) taskgraphMap.get(iTaskInfo);

        LOGGER.info("Key:" + iTaskInfo.taskName() + "\t" + "Value:" + value + "\n");
        for (int i = 0; i < value.size(); i++) {
          TaskInfo taskInfoVal = (TaskInfo) value.get(i);
          //this.generateITaskGraph(taskInfoVal.dataflowOperation,
          //    taskInfoVal.sourceTask, taskInfoVal.targetTask);
          dataflowTaskGraphGenerator.generateITaskGraph(
              taskInfoVal.dataflowOperation, taskInfoVal.sourceTask, taskInfoVal.targetTask);
        }
      }
    } catch (ClassCastException cce) {
      cce.printStackTrace();
    } catch (NoSuchElementException nsee) {
      nsee.printStackTrace();
    }
    return this;
  }

  @Override
  public ITaskGraphGenerate generateITaskGraph(DataflowOperation dataflowOperation,
                                               ITaskInfo taskVertex, ITaskInfo... taskEdge) {
    return this;
  }

  /*@Override
  public ITaskGraphGenerate generateITaskGraph(DataflowOperation dataflowOperation,
                                               ITaskInfo taskVertex,
                                               ITaskInfo... taskEdge) {
    try {
      this.iTaskGraph.addTaskVertex(taskVertex);
      if (taskEdge.length >= 1) {
        this.iTaskGraph.addTaskVertex(taskEdge[0]);
        for (ITaskInfo mapperTask : taskEdge) {
          this.iTaskGraph.addTaskEdge(taskVertex, taskEdge[0], dataflowOperation);
        }
      }
    } catch (IllegalArgumentException iae) {
      iae.printStackTrace();
    }
    return this;
  }*/

  /**
   * It is for displaying the connected task graph objects and task vertexes
   * in the taskgraph.
   */
  @Override
  public void displayTaskGraph() {
    LOGGER.info("\n Task Connected List:" + taskConnectedList + "\n\n");
    LOGGER.info("Task Connected List:" + taskInfoConnectedList + "\n\n");
    LOGGER.info("Generated Task Graph Value Is:" + this.iTaskGraph.getTaskVertexSet());
  }

  /**
   * This method will submit the taskgraph to the task graph parser for parsing
   * the task graph and preparing the task for execution.
   */
  @Override
  public ITaskGraphGenerate submit() {
    return this;
  }

  /*public ITaskGraphGenerate generateTaskEdge() {
    return this;
  }*/

  public ITaskGraphGenerate submitToTaskGraphGenerator() {
    return this;
  }

  //The methods defined below will be removed and just keep it for reference....
  public ITaskGraphGenerate parseTaskGraph() {
    return this;
  }

  public ITaskGraphGenerate generateExecutionGraph(ITaskGraphGenerate taskGraph) {
    return this;
  }

  public ITaskGraphGenerate submitToTaskExecutor() {
    return this;
  }
}
