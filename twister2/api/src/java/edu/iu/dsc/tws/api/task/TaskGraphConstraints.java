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
package edu.iu.dsc.tws.api.task;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.Vertex;

public class TaskGraphConstraints {

  private static final Logger LOG = Logger.getLogger(TaskGraphConstraints.class.getName());

  private String nodeName;

  private Map<String, Map<String, Object>> inputs = new HashMap<>();

  public TaskGraphConstraints(String name) {
    this.nodeName = name;
  }


  void build(DataFlowTaskGraph graph) {
    for (Map.Entry<String, Map<String, Object>> e : inputs.entrySet()) {
      Vertex vertex = graph.vertex(nodeName);
      if (vertex == null) {
        throw new RuntimeException("Task Vertex Is Empty:" + nodeName);
      }
      LOG.info("constraints:" + e.getKey() + "\t" + e.getValue());
      //graph.addTaskConstraints(e.getKey(), e.getValue());
      graph.addTaskConstraints(e.getKey(), e.getValue());
    }
  }

//  void build(DataFlowTaskGraph graph) {
//    for (Map.Entry<String, Edge> e : inputs.entrySet()) {
//      Vertex v1 = graph.vertex(nodeName);
//      if (v1 == null) {
//        throw new RuntimeException("Failed to connect non-existing task: " + nodeName);
//      }
//
//      Vertex v2 = graph.vertex(e.getKey());
//      if (v2 == null) {
//        throw new RuntimeException("Failed to connect non-existing task: " + e.getKey());
//      }
//      //graph.addTaskEdge(v2, v1, e.getValue());
//      graph.addConstraints(e.getKey(), e.getValue());
//    }
//  }

}
