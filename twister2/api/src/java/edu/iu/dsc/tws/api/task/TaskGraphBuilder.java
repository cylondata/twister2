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
public class TaskGraphBuilder {
  private static final Logger LOG = Logger.getLogger(TaskGraphBuilder.class.getName());

  private Config config;

  private Map<String, Vertex> nodes = new HashMap<>();

  private List<ComputeConnection> computeConnections = new ArrayList<>();

  private List<SourceConnection> sourceConnections = new ArrayList<>();

  private int defaultParallelism;

  private OperationMode mode;

  public TaskGraphBuilder(Config cfg) {
    this.config = cfg;
    this.defaultParallelism = TaskContext.getDefaultParallelism(cfg, 1);
  }

  public ComputeConnection addSink(String name, ISink sink) {
    Vertex vertex = new Vertex(name, sink, defaultParallelism);
    nodes.put(name, vertex);

    return new ComputeConnection(name);
  }

  public ComputeConnection addSink(String name, ISink sink, int parallel) {
    Vertex vertex = new Vertex(name, sink, parallel);
    nodes.put(name, vertex);

    return new ComputeConnection(name);
  }

  public ComputeConnection addCompute(String name, ICompute compute) {
    Vertex vertex = new Vertex(name, compute, defaultParallelism);
    nodes.put(name, vertex);

    return new ComputeConnection(name);
  }

  public ComputeConnection addCompute(String name, ICompute compute, int parallel) {
    Vertex vertex = new Vertex(name, compute, parallel);
    nodes.put(name, vertex);

    return new ComputeConnection(name);
  }

  public SourceConnection addSource(String name, ISource source) {
    Vertex vertex = new Vertex(name, source, defaultParallelism);
    nodes.put(name, vertex);

    return new SourceConnection(name);
  }

  public DataFlowTaskGraph build() {
    DataFlowTaskGraph graph = new DataFlowTaskGraph();

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
