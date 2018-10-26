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
package edu.iu.dsc.tws.task.graph;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

public class HierarchicalTaskGraph extends DataFlowTaskGraph {

  private static final Logger LOG = Logger.getLogger(HierarchicalTaskGraph.class.getName());

  private Map<String, DataFlowTaskGraph> taskGraphMap = new HashMap<>();
  private Set<DataFlowTaskGraph> taskGraph = new HashSet<>();

  public void addTaskGraph(String name, DataFlowTaskGraph dataFlowTaskGraph) {
    if (!validateTaskGraph(name)) {
      addTaskGraph(dataFlowTaskGraph);
      taskGraphMap.put(name, dataFlowTaskGraph);
    }
  }

  private boolean validateTaskGraph(String taskgraphName) {
    boolean flag = false;
    if (taskGraphMap.containsKey(taskgraphName)) {
      throw new RuntimeException("Duplicate names for the submitted graph:" + taskgraphName);
    }
    return flag;
  }

  public boolean addTaskGraph(DataFlowTaskGraph dataFlowTaskGraph) {
    if (dataFlowTaskGraph == null) {
      throw new NullPointerException();
    } else if (this.containsTaskGraph(dataFlowTaskGraph)) {
      return false;
    } else {
      this.taskGraph.add(dataFlowTaskGraph);
      return true;
    }
  }

  private boolean containsTaskGraph(DataFlowTaskGraph dataFlowTaskGraph) {
    return taskGraph.contains(dataFlowTaskGraph);
  }

  public Set<DataFlowTaskGraph> getTaskGraphSet() {
    return taskGraph;
  }
}
