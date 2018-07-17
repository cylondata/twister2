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

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.Vertex;

public final class TaskVertexParser {

  private static final Logger LOG = Logger.getLogger(TaskVertexParser.class.getName());

  protected TaskVertexParser() {
  }

  /**
   * This method is mainly used to parse the task vertex set and the taskgraph and
   * identify the source, parent, child, and sink tasks. FinallyAnd, the identified tasks
   * be stored in a separate Set.
   */
  @SuppressWarnings("unchecked")
  public static List<Set<Vertex>> parseVertexSet(Set<Vertex> taskVertexSet,
                                                 DataFlowTaskGraph dataFlowTaskGraph) {

    LOG.info(" I am at parse V.Set:" + taskVertexSet.size());

    List<Set<Vertex>> taskVertexList = new ArrayList<>();
    for (Vertex vertex : taskVertexSet) {
      if (dataFlowTaskGraph.outgoingTaskEdgesOf(vertex).size() >= 2) {
        Set<Vertex> parentTask = new LinkedHashSet<>();
        parentTask.add(vertex);
        taskVertexList.add(parentTask);
        LinkedHashSet<Vertex> vertexSet = (LinkedHashSet) dataFlowTaskGraph.childrenOfTask(vertex);
        taskVertexList.add(vertexSet);
      } else if (dataFlowTaskGraph.incomingTaskEdgesOf(vertex).size() >= 2) {
        Set<Vertex> parentTask1 = new LinkedHashSet<>();
        for (int i = 0; i < taskVertexList.size(); i++) {
          Set<Vertex> vv = taskVertexList.get(i);
          for (Vertex vertex1 : vv) {
            if (!vertex1.getName().equals(vv) && !parentTask1.contains(vertex)) {
              parentTask1.add(vertex);
              taskVertexList.add(parentTask1);
            }
          }
        }
      } else if (!(dataFlowTaskGraph.incomingTaskEdgesOf(vertex).size() >= 2)
          && !(dataFlowTaskGraph.outgoingTaskEdgesOf(vertex).size() >= 2)) {
        Set<Vertex> parentTask1 = new LinkedHashSet<>();
        for (int i = 1; i < taskVertexList.size(); i++) {
          Set<Vertex> vv = taskVertexList.get(i);
          if (vv.contains(vertex)) {
            break;
          } else {
            for (Vertex vertex1 : vv) {
              if (!vertex1.getName().equals(vertex.getName())
                  && !parentTask1.contains(vertex)
                  && !vertex1.equals(vertex)) {
                parentTask1.add(vertex);
                taskVertexList.add(parentTask1);
              }
            }
          }
        }
      }
    }

    LOG.info("task vertex list:" + taskVertexList);
    return taskVertexList;
  }
}
