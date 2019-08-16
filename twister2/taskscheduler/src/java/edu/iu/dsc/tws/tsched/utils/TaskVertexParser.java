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

import edu.iu.dsc.tws.api.compute.graph.ComputeGraph;
import edu.iu.dsc.tws.api.compute.graph.Vertex;

/**
 * This class acts as a helper class for the batch task scheduling algorithms to parse the simple
 * to complex task graph.
 */
public final class TaskVertexParser {

  private static final Logger LOG = Logger.getLogger(TaskVertexParser.class.getName());

  private List<Set<Vertex>> taskVertexList = new LinkedList<>();

  private Set<Vertex> targetVertexSet = new LinkedHashSet<>();

  public List<Set<Vertex>> parseVertexSet(ComputeGraph computeGraph) {
    Set<Vertex> taskVertexSet = computeGraph.getTaskVertexSet();
    for (Vertex vertex : taskVertexSet) {
      if (computeGraph.inDegreeOfTask(vertex) == 0) {
        add(vertex);
        targetVertexSet.add(vertex);
        if (computeGraph.childrenOfTask(vertex).size() >= 1) {
          checkChildTasks(computeGraph, vertex);
        }
      } else {
        if (checkChildTasks(computeGraph, vertex)) {
          if (!targetVertexSet.contains(vertex)) {
            add(vertex);
            targetVertexSet.add(vertex);
          }
        }
      }
    }

    for (Set<Vertex> aTaskVertexSet : taskVertexList) {
      for (Vertex vertex : aTaskVertexSet) {
        LOG.fine("%%% Vertex Details:" + vertex.getName() + "\t" + aTaskVertexSet.size());
      }
    }
    return taskVertexList;
  }

  private boolean checkChildTasks(ComputeGraph computeGraph, Vertex vertex) {
    boolean flag = false;
    if (computeGraph.outDegreeOfTask(vertex) >= 1) {
      Set<Vertex> childTask = computeGraph.childrenOfTask(vertex);
      if (!targetVertexSet.containsAll(childTask)) {
        add(childTask);
        targetVertexSet.addAll(childTask);
      }
    } else {
      flag = true;
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
