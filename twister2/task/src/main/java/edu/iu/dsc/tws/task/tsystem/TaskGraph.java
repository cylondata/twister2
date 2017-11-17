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
package edu.iu.dsc.tws.task.tsystem;

import org.jgrapht.DirectedGraph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;

import tasksystem.Task;
import tasksystem.TaskConfiguration;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TaskGraph{

  private Set<AbstractMapper> runningTasks = new HashSet<> ();

  //At present, we are using JGrapht for implementing the task graph

  private DirectedGraph<AbstractMapper, DefaultEdge> taskVertices =
                  new DefaultDirectedGraph<AbstractMapper, DefaultEdge> (DefaultEdge.class);

  private DirectedGraph<AbstractMapper, CManager> tVertices =
                  new DefaultDirectedGraph<AbstractMapper, CManager> (CManager.class);

  public static int totalNumberOfTasks = 0;


  public DirectedGraph<AbstractMapper, DefaultEdge> getTaskVertices() {
    return taskVertices;
  }

  public void setTaskVertices(DirectedGraph<AbstractMapper, DefaultEdge> taskVertices) {
    this.taskVertices = taskVertices;
  }

  public DirectedGraph<AbstractMapper, CManager> getTVertices() {
    return tVertices;
  }

  public void setTVertices(DirectedGraph<AbstractMapper, CManager> tVertices) {
    this.tVertices = tVertices;
  }

  public TaskGraph generateTaskGraph(AbstractMapper mapperTask1, AbstractMapper... mapperTask2) {
    try {
      this.taskVertices.addVertex (mapperTask1);
      for (AbstractMapper mapperTask : mapperTask2) {
        this.taskVertices.addEdge (mapperTask, mapperTask1);
      }
    } catch (IllegalArgumentException iae) {
      iae.printStackTrace ();
    }
    System.out.println ("Generated Task Graph with Vertices is:" + taskVertices.vertexSet ().size ());
    ++totalNumberOfTasks;
    return this;
  }

  //public TaskGraph generateDataflowGraph(AbstractMapper mapperTask1, AbstractMapper mapperTask2, CManager ...cManagerTask){
  public TaskGraph generateDataflowGraph(AbstractMapper mapperTask1, AbstractMapper mapperTask2, CManager ...cManagerTask){

    try {
      this.tVertices.addVertex (mapperTask1);
      this.tVertices.addVertex (mapperTask2);
      System.out.println("Task vertices are:"+this.tVertices.vertexSet ());
        //for (AbstractMapper mapperTask : mapperTask1) {
        //  this.tVertices.addEdge (mapperTask, mapperTask1, cManagerTask);
        //}
      this.tVertices.addEdge (mapperTask1, mapperTask2, cManagerTask[0]);
    } catch (Exception iae) {
      iae.printStackTrace ();
    }
    System.out.println("Generated Dataflow Graph is:"+this.toString ());
    return this;
  }


}