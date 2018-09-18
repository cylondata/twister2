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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.task.api.ICompute;
import edu.iu.dsc.tws.task.api.ISink;
import edu.iu.dsc.tws.task.api.ISource;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.OperationMode;
import edu.iu.dsc.tws.task.graph.Vertex;

/**
 * This is the entry point for creating a task graph by the user.
 */
public final class TaskGraphBuilder {
  private static final Logger LOG = Logger.getLogger(TaskGraphBuilder.class.getName());

  /**
   * Keep track of the nodes with their names
   */
  private Map<String, Vertex> nodes = new HashMap<>();

  /**
   * The parent edges of a node
   */
  private List<ComputeConnection> computeConnections = new ArrayList<>();

  /**
   * Source connections
   */
  private List<SourceConnection> sourceConnections = new ArrayList<>();

  /**
   * Default parallelism read from a configuration parameter
   */
  private int defaultParallelism;

  /**
   * The execution mode
   */
  private OperationMode mode = OperationMode.STREAMING;

  /**
   * Create an instance of the task builder.
   * @param cfg configuration
   * @return new task graph builder instance
   */
  public static TaskGraphBuilder newBuilder(Config cfg) {
    return new TaskGraphBuilder(cfg);
  }

  private TaskGraphBuilder(Config cfg) {
    this.defaultParallelism = TaskConfigurations.getDefaultParallelism(cfg, 1);
  }

  /**
   * Set the operation mode, we default to streaming mode
   * @param mode the operation mode (streaming, batch)
   */
  public void setMode(OperationMode mode) {
    this.mode = mode;
  }

  /**
   * Add a sink node to the graph
   *
   * @param name name of the node
   * @param sink implementation of the node
   * @return a compute connection, that can be used to connect this node to other nodes as a child
   */
  public ComputeConnection addSink(String name, ISink sink) {
    Vertex vertex = new Vertex(name, sink, defaultParallelism);
    nodes.put(name, vertex);

    return createComputeConnection(name);
  }

  /**
   * Add a sink node to the graph
   *
   * @param name name of the node
   * @param sink implementation of the node
   * @param parallel number of parallel instances
   * @return a compute connection, that can be used to connect this node to other nodes as a child
   */
  public ComputeConnection addSink(String name, ISink sink, int parallel) {
    Vertex vertex = new Vertex(name, sink, parallel);
    nodes.put(name, vertex);

    return createComputeConnection(name);
  }

  /**
   * Add a compute node to the graph
   *
   * @param name name of the node
   * @param compute number of parallel instances
   * @return a compute connection, that can be used to connect this node to other nodes as a child
   */
  public ComputeConnection addCompute(String name, ICompute compute) {
    Vertex vertex = new Vertex(name, compute, defaultParallelism);
    nodes.put(name, vertex);

    return createComputeConnection(name);
  }

  /**
   * Add a compute node to the graph
   *
   * @param name name of the node
   * @param compute implementation of the node
   * @param parallel number of parallel instances
   * @return a compute connection, that can be used to connect this node to other nodes as a child
   */
  public ComputeConnection addCompute(String name, ICompute compute, int parallel) {
    Vertex vertex = new Vertex(name, compute, parallel);
    nodes.put(name, vertex);

    return createComputeConnection(name);
  }

  /**
   * Create a compute connection
   *
   * @param name name of the connection
   * @return a compute connection, that can be used to connect this node to other nodes as a child
   */
  private ComputeConnection createComputeConnection(String name) {
    ComputeConnection cc = new ComputeConnection(name);
    computeConnections.add(cc);
    return cc;
  }

  /**
   * Add a source node to the graph
   *
   * @param name name of the node
   * @param source implementation of the node
   * @return a compute connection, that can be used to connect this node to other nodes as a child
   */
  public SourceConnection addSource(String name, ISource source) {
    Vertex vertex = new Vertex(name, source, defaultParallelism);
    nodes.put(name, vertex);

    return createSourceConnection(name);
  }

  /**
   * Add a source node to the graph
   *
   * @param name name of the node
   * @param source implementation of the node
   * @param parllel parallelism of the node
   * @return a compute connection, that can be used to connect this node to other nodes as a child
   */
  public SourceConnection addSource(String name, ISource source, int parllel) {
    Vertex vertex = new Vertex(name, source, parllel);
    nodes.put(name, vertex);

    return createSourceConnection(name);
  }

  private SourceConnection createSourceConnection(String name) {
    SourceConnection sc = new SourceConnection(name);
    sourceConnections.add(sc);
    return sc;
  }

  public DataFlowTaskGraph build() {
    DataFlowTaskGraph graph = new DataFlowTaskGraph();
    graph.setOperationMode(mode);

    for (Map.Entry<String, Vertex> e : nodes.entrySet()) {
      graph.addTaskVertex(e.getKey(), e.getValue());
    }

    for (ComputeConnection c : computeConnections) {
      c.build(graph);
    }

    for (SourceConnection c : sourceConnections) {
      c.build(graph);
    }

    return graph;
  }
}
