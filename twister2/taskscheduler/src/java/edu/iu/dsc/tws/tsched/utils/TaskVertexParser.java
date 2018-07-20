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

/**
 * This class acts as a helper class for the batch task scheduling algorithms to
 * parse the simple dataflow task graph to the complex task graph.
 */
public final class TaskVertexParser {

  private static final Logger LOG = Logger.getLogger(TaskVertexParser.class.getName());

  protected TaskVertexParser() {
  }

  /**
   * This method is mainly used to parse the task vertex set and the taskgraph and
   * identify the source, parent, child, and sink tasks. Finally, the identified tasks
   * will be stored in a separate Set.
   */
  @SuppressWarnings("unchecked")
  public static List<Set<Vertex>> parseVertexSet(Set<Vertex> taskVertexSet,
                                                 DataFlowTaskGraph dataFlowTaskGraph) {
    //This logic could be optimized later...!
    List<Set<Vertex>> taskVertexList = new ArrayList<>();

    for (Vertex vertex : taskVertexSet) {
      if (dataFlowTaskGraph.outgoingTaskEdgesOf(vertex).size() > 1) {

        Set<Vertex> vertexSet = new LinkedHashSet<>();
        vertexSet.add(vertex);
        taskVertexList.add(vertexSet);

        LinkedHashSet<Vertex> childrenOfTaskSet =
            (LinkedHashSet) dataFlowTaskGraph.childrenOfTask(vertex);
        taskVertexList.add(childrenOfTaskSet);
      } else if (dataFlowTaskGraph.incomingTaskEdgesOf(vertex).size() > 1) {

        Set<Vertex> vertexSet = new LinkedHashSet<>();
        for (int i = 0; i < taskVertexList.size(); i++) {
          Set<Vertex> vertices = taskVertexList.get(i);
          for (Vertex vertex1 : vertices) {
            if (!vertex1.getName().equals(vertices) && !vertexSet.contains(vertex)) {
              vertexSet.add(vertex);
              taskVertexList.add(vertexSet);
            }
          }
        }
      } else if (!(dataFlowTaskGraph.incomingTaskEdgesOf(vertex).size() > 1)
          || !(dataFlowTaskGraph.outgoingTaskEdgesOf(vertex).size() > 1)) {

        Set<Vertex> vertexSet = new LinkedHashSet<>();

        if (taskVertexList.size() == 0) {
          vertexSet.add(vertex);
          taskVertexList.add(vertexSet);

        } else if (taskVertexList.size() == 1) {

          Set<Vertex> vv = taskVertexList.get(0);
          if (!vertexSet.contains(vv) || !vv.contains(vertex)) {
            vertexSet.add(vertex);
            taskVertexList.add(vertexSet);
          }
        } else {

          int size = taskVertexList.size();

          for (int i = size - 1; i < taskVertexList.size(); i++) {
            Set<Vertex> vv = taskVertexList.get(i);
            if (!vv.contains(vertex)) {
              for (Vertex vertex1 : vv) {
                if (!vertex1.getName().equals(vertex.getName()) && !vertexSet.contains(vertex)
                    && !vertexSet.contains(vertex1) && !vertex1.equals(vertex)) {
                  vertexSet.add(vertex);
                  taskVertexList.add(vertexSet);
                }
              }
            }
          }

        }
      }//End of Inner Else block*/
    }//End of Outer For

    LOG.info("Batch Task Vertex List:" + taskVertexList);
    return taskVertexList;
  }
}
