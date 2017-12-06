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

  private IDataflowTaskGraph<Mapper, DefaultTaskEdge> taskGraph =
      new DataflowTaskGraph<>(DefaultTaskEdge.class);

  private Set<Mapper> runningTasks = new HashSet<>();

  public IDataflowTaskGraph<Mapper, CManager> getDataflowTaskGraph() {
    return dataflowTaskGraph;
  }

  public void setDataflowTaskGraph(IDataflowTaskGraph<Mapper, CManager>
                                       dataflowTaskGraph) {
    this.dataflowTaskGraph = dataflowTaskGraph;
  }

  public IDataflowTaskGraph<Mapper, DefaultTaskEdge> getTaskGraph() {
    return taskGraph;
  }

  public void setTaskGraph(IDataflowTaskGraph<Mapper,
      DefaultTaskEdge> taskGraph) {
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

  public synchronized Mapper[] getReadyTasks() {
    return this.dataflowTaskGraph.getTaskVertexSet().stream()
        .filter(task -> !this.runningTasks.contains(task)
            && this.dataflowTaskGraph.inDegreeOf(task) == 0)
        .toArray(size -> new Mapper[size]);
  }

  public synchronized void notifyDone(Mapper mapperTask) {
    System.out.println("Mapper task done to be removed:" + mapperTask);
    this.runningTasks.remove(mapperTask);
    this.dataflowTaskGraph.removeTaskVertex(mapperTask);
  }

  public void setAsStarted(final Mapper task) {
    this.runningTasks.add(task);
  }

  public synchronized boolean isDone() {
    return this.dataflowTaskGraph.getTaskVertexSet().isEmpty();
  }
}

