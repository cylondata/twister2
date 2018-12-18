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
package edu.iu.dsc.tws.api.task.htg;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.task.TaskConfigurations;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.OperationMode;
import edu.iu.dsc.tws.task.graph.htg.HierarchicalTaskGraph;

/**
 * This is the entry point for creating a task graph by the user.
 */
public final class HTGBuilder {

  private static final Logger LOG = Logger.getLogger(HTGBuilder.class.getName());

  /**
   * Keep track of the dataflow taskgraph with their names
   */
  private Map<String, DataFlowTaskGraph> graphMap = new HashMap<>();

  /**
   * The parent edges of a taskgraph
   */
  private List<HTGComputeConnection> computeTaskGraphConnections = new ArrayList<>();

  /**
   * Source task graph connections
   */
  private List<HTGSourceConnection> sourceTaskGraphConnections = new ArrayList<>();

  /**
   * Default parallelism read from a configuration parameter
   */
  private int defaultParallelism;

  /**
   * The execution mode
   */
  private OperationMode operationMode = OperationMode.STREAMING;

  /**
   * Create an instance of the task graph builder.
   *
   * @param cfg configuration
   * @return new task graph builder instance
   */
  public static HTGBuilder newBuilder(Config cfg) {
    return new HTGBuilder(cfg);
  }

  private HTGBuilder(Config cfg) {
    this.defaultParallelism = TaskConfigurations.getDefaultParallelism(cfg, 1);
  }

  public void setMode(OperationMode mode) {
    this.operationMode = mode;
  }

  public HTGSourceConnection addSourceTaskGraph(String name,
                                                DataFlowTaskGraph dataFlowTaskGraph) {
    dataFlowTaskGraph.setTaskGraphName(name);
    graphMap.put(name, dataFlowTaskGraph);
    return createSourceTaskGraphConnection(name);
  }

  private HTGSourceConnection createSourceTaskGraphConnection(String name) {
    HTGSourceConnection sc = new HTGSourceConnection(name);
    sourceTaskGraphConnections.add(sc);
    return sc;
  }

  public HTGComputeConnection addSinkTaskGraph(String name,
                                               DataFlowTaskGraph dataFlowTaskGraph, String task) {
    dataFlowTaskGraph.setTaskGraphName(name);
    graphMap.put(name, dataFlowTaskGraph);
    return createComputeTaskGraphConnection(name, task);
  }

  private HTGComputeConnection createComputeTaskGraphConnection(String name, String task) {
    HTGComputeConnection cc = new HTGComputeConnection(name, task);
    computeTaskGraphConnections.add(cc);
    return cc;
  }

  public HierarchicalTaskGraph buildHierarchicalTaskGraph() {

    HierarchicalTaskGraph hierarchicalTaskGraph = new HierarchicalTaskGraph();
    hierarchicalTaskGraph.setOperationMode(operationMode);

    for (Map.Entry<String, DataFlowTaskGraph> e : graphMap.entrySet()) {
      hierarchicalTaskGraph.addTaskGraph(e.getKey(), e.getValue());
      LOG.info("key and value:" + e.getKey() + "\t" + e.getValue());
    }

    for (HTGComputeConnection htgComputeConnection : computeTaskGraphConnections) {
      htgComputeConnection.build(hierarchicalTaskGraph);
    }

    for (HTGSourceConnection c : sourceTaskGraphConnections) {
      c.build(hierarchicalTaskGraph);
    }

    LOG.info("HTG Values:" + hierarchicalTaskGraph.getTaskGraphSet());
    return hierarchicalTaskGraph;
  }
}
