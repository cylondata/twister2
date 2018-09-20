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

import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.task.api.ICompute;
import edu.iu.dsc.tws.task.api.IFunction;
import edu.iu.dsc.tws.task.api.ISink;
import edu.iu.dsc.tws.task.api.ISource;

/**
 * This class is responsible for building the task graph and the configuration values required for
 * each task in the task graph.
 */
public final class GraphBuilder {
  private DataFlowTaskGraph graph;

  private GraphBuilder() {
    graph = new DataFlowTaskGraph();
  }

  public static GraphBuilder newBuilder() {
    return new GraphBuilder();
  }

  public GraphBuilder addSource(String name, ISource source) {
    graph.addTaskVertex(name, new Vertex(name, source));
    return this;
  }

  public GraphBuilder addSink(String name, ISink sink) {
    graph.addTaskVertex(name, new Vertex(name, sink));
    return this;
  }

  public GraphBuilder addTask(String name, ICompute task) {
    graph.addTaskVertex(name, new Vertex(name, task));
    return this;
  }

  public GraphBuilder setParallelism(String taskName, int parallel) {
    Vertex v = graph.vertex(taskName);
    if (v == null) {
      throw new RuntimeException("Failed to add configuration to non-existing task: " + taskName);
    }
    v.setParallelism(parallel);
    return this;
  }

  public GraphBuilder addConfiguration(String taskName, String property, Object value) {
    Vertex v = graph.vertex(taskName);
    if (v == null) {
      throw new RuntimeException("Failed to add configuration to non-existing task: " + taskName);
    }
    v.addConfiguration(property, value);
    return this;
  }

  public GraphBuilder connect(String t1, String t2, String name) {
    Vertex v1 = graph.vertex(t1);
    if (v1 == null) {
      throw new RuntimeException("Failed to connect non-existing task: " + t1);
    }

    Vertex v2 = graph.vertex(t2);
    if (v2 == null) {
      throw new RuntimeException("Failed to connect non-existing task: " + t2);
    }
    graph.addTaskEdge(v1, v2, new Edge(name));
    return this;
  }

  public GraphBuilder connect(String t1, String t2, String name, String operation) {
    Vertex v1 = graph.vertex(t1);
    if (v1 == null) {
      throw new RuntimeException("Failed to connect non-existing task: " + t1);
    }

    Vertex v2 = graph.vertex(t2);
    if (v2 == null) {
      throw new RuntimeException("Failed to connect non-existing task: " + t2);
    }
    graph.addTaskEdge(v1, v2, new Edge(name, operation));
    return this;
  }

  public GraphBuilder connect(String t1, String t2, String name, String operation, IFunction task) {
    Vertex v1 = graph.vertex(t1);
    if (v1 == null) {
      throw new RuntimeException("Failed to connect non-existing task: " + t1);
    }

    Vertex v2 = graph.vertex(t2);
    if (v2 == null) {
      throw new RuntimeException("Failed to connect non-existing task: " + t2);
    }
    graph.addTaskEdge(v1, v2, new Edge(name, operation, task));
    return this;
  }

  public GraphBuilder connect(String t1, String t2, String name, String operation,
                              DataType dataType, DataType keyType) {
    Vertex v1 = graph.vertex(t1);
    if (v1 == null) {
      throw new RuntimeException("Failed to connect non-existing task: " + t1);
    }

    Vertex v2 = graph.vertex(t2);
    if (v2 == null) {
      throw new RuntimeException("Failed to connect non-existing task: " + t2);
    }
    graph.addTaskEdge(v1, v2, new Edge(name, operation, dataType, keyType));
    return this;
  }

  public GraphBuilder connect(String t1, String t2, String name, String operation,
                              IFunction function, DataType dataType, DataType keyType) {
    Vertex v1 = graph.vertex(t1);
    if (v1 == null) {
      throw new RuntimeException("Failed to connect non-existing task: " + t1);
    }

    Vertex v2 = graph.vertex(t2);
    if (v2 == null) {
      throw new RuntimeException("Failed to connect non-existing task: " + t2);
    }
    graph.addTaskEdge(v1, v2, new Edge(name, operation, dataType, keyType, function));
    return this;
  }

  public GraphBuilder connect(String t1, String t2, String name, String operation,
                              DataType dataType) {
    Vertex v1 = graph.vertex(t1);
    if (v1 == null) {
      throw new RuntimeException("Failed to connect non-existing task: " + t1);
    }

    Vertex v2 = graph.vertex(t2);
    if (v2 == null) {
      throw new RuntimeException("Failed to connect non-existing task: " + t2);
    }
    graph.addTaskEdge(v1, v2, new Edge(name, operation, dataType));
    return this;
  }

  public DataFlowTaskGraph build() {
    graph.validate();
    graph.build();
    return graph;
  }

  /**
   * Set the operation mode of the graph, default is set to stream
   * @param mode
   * @return
   */
  public DataFlowTaskGraph operationMode(OperationMode mode) {
    graph.setOperationMode(mode);
    return graph;
  }
}
