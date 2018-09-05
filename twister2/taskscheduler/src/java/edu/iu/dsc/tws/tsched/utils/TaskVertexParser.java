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

  private static final Logger LOGGER = Logger.getLogger(TaskVertexParser.class.getName());

  private static List<Set<Vertex>> taskVertexList = new LinkedList<>();

  protected TaskVertexParser() {
  }

  /**
   * This method is mainly used to parse the task vertex set and the taskgraph and identify the
   * source, parent, child, and sink tasks. Finally, the identified tasks will be stored in a
   * separate Set.
   */
  @SuppressWarnings("unchecked")
  public static List<Set<Vertex>> parseVertexSet(DataFlowTaskGraph dataFlowTaskGraph) {

    Set<Vertex> taskVertexSet = dataFlowTaskGraph.getTaskVertexSet();

    for (Vertex vertex : taskVertexSet) {

      if (dataFlowTaskGraph.inDegreeOfTask(vertex) == 0) {
        add(vertex);
        if (!dataFlowTaskGraph.childrenOfTask(vertex).isEmpty()) {
          add(dataFlowTaskGraph.childrenOfTask(vertex));
        }
      } else if (dataFlowTaskGraph.outgoingTaskEdgesOf(vertex).size() > 1) {
        add(dataFlowTaskGraph.childrenOfTask(vertex));
      } else if (dataFlowTaskGraph.incomingTaskEdgesOf(vertex).size() > 1) {
        add(vertex);
      } else if ((dataFlowTaskGraph.incomingTaskEdgesOf(vertex).size() == 1)
              && (dataFlowTaskGraph.outgoingTaskEdgesOf(vertex).size() == 0)) {
        add(vertex);
      }
    }
    LOGGER.fine("Batch Task Vertex List:" + taskVertexList);
    return taskVertexList;
  }

  private static void add(Vertex vertex) {
    Set<Vertex> vSet = new LinkedHashSet<>();
    vSet.add(vertex);
    taskVertexList.add(vSet);
  }

  private static void add(Set<Vertex> taskVertexSet) {
    taskVertexList.add(taskVertexSet);
  }
}
