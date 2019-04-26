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
package edu.iu.dsc.tws.tsched.utils;

import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.Vertex;

/**
 * This class acts as a helper class for the batch task scheduling algorithms to parse the simple
 * to complex task graph.
 */
public final class TaskVertexParser {

  private static final Logger LOG = Logger.getLogger(TaskVertexParser.class.getName());


  private List<Set<Vertex>> taskVertexList = new LinkedList<>();
  private Set<Vertex> targetVertexSet = new LinkedHashSet<>();

  public List<Set<Vertex>> parseVertexSet(DataFlowTaskGraph dataFlowTaskGraph) {
    Set<Vertex> taskVertexSet = dataFlowTaskGraph.getTaskVertexSet();
    for (Vertex vertex : taskVertexSet) {
      if (dataFlowTaskGraph.inDegreeOfTask(vertex) == 0) {
        add(vertex);
        targetVertexSet.add(vertex);
        checkChildTasks(dataFlowTaskGraph, vertex);
      } else {
        if (checkChildTasks(dataFlowTaskGraph, vertex)) {
          add(vertex);
          targetVertexSet.add(vertex);
        }
      }
    }
    for (Set<Vertex> aTaskVertexSet : taskVertexList) {
      for (Vertex vertex : aTaskVertexSet) {
        LOG.fine("%%% Vertex Details:" + aTaskVertexSet.size() + "\t" + vertex.getName());
      }
    }
    return taskVertexList;
  }

  private boolean checkChildTasks(DataFlowTaskGraph dataFlowTaskGraph, Vertex vertex) {
    boolean flag = false;
    if (dataFlowTaskGraph.outDegreeOfTask(vertex) >= 1) {
      Set<Vertex> childTask = dataFlowTaskGraph.childrenOfTask(vertex);
      if (dataFlowTaskGraph.parentsOfTask(vertex.getName()).size() <= 1) {
        add(childTask);
        targetVertexSet.addAll(childTask);
      } else {
        if (!targetVertexSet.containsAll(childTask)) {
          add(childTask);
          targetVertexSet.addAll(childTask);
        }
      }
    }
    return flag;
  }

  private void add(Vertex vertex) {
    Set<Vertex> vertexSet = new LinkedHashSet<>();
    vertexSet.add(vertex);
    taskVertexList.add(vertexSet);
  }

  private void add(Set<Vertex> vertexSet) {
    taskVertexList.add(vertexSet);
  }
}
