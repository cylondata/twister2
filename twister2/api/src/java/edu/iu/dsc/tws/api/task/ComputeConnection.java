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

import edu.iu.dsc.tws.api.task.function.ReduceFn;
import edu.iu.dsc.tws.comms.api.Op;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.executor.core.OperationNames;
import edu.iu.dsc.tws.task.api.IFunction;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.Edge;
import edu.iu.dsc.tws.task.graph.Vertex;

public class ComputeConnection {
  private String nodeName;

  private Map<String, Edge> inputs = new HashMap<>();

  public ComputeConnection(String nodeName) {
    this.nodeName = nodeName;
  }

  public ComputeConnection broadcast(String parent) {
    Edge edge = new Edge(TaskContext.DEFAULT_EDGE, OperationNames.BROADCAST);
    inputs.put(parent, edge);

    return this;
  }

  public ComputeConnection reduce(String parent, IFunction function) {
    Edge edge = new Edge(TaskContext.DEFAULT_EDGE, OperationNames.REDUCE, function);
    inputs.put(parent, edge);

    return this;
  }

  public ComputeConnection reduce(String parent, Op op) {
    Edge edge = new Edge(TaskContext.DEFAULT_EDGE, OperationNames.REDUCE,
        new ReduceFn(op, DataType.OBJECT));
    inputs.put(parent, edge);

    return this;
  }

  public ComputeConnection reduce(String parent, IFunction function, DataType dataType) {
    Edge edge = new Edge(TaskContext.DEFAULT_EDGE, OperationNames.REDUCE, dataType, function);
    inputs.put(parent, edge);

    return this;
  }

  public ComputeConnection reduce(String parent, String name, IFunction function) {
    Edge edge = new Edge(name, OperationNames.REDUCE, DataType.OBJECT, function);
    inputs.put(parent, edge);

    return this;
  }

  public ComputeConnection reduce(String parent, String name, Op op) {
    Edge edge = new Edge(name, OperationNames.REDUCE, DataType.OBJECT,
        new ReduceFn(op, DataType.OBJECT));
    inputs.put(parent, edge);

    return this;
  }

  public ComputeConnection reduce(String parent, String name, Op op, DataType dataType) {
    Edge edge = new Edge(name, OperationNames.REDUCE, dataType,
        new ReduceFn(op, dataType));
    inputs.put(parent, edge);

    return this;
  }

  public ComputeConnection reduce(String parent, String name,
                                  IFunction function, DataType dataType) {
    Edge edge = new Edge(name, OperationNames.REDUCE, dataType, function);
    inputs.put(parent, edge);

    return this;
  }


  public ComputeConnection gather(String parent) {
    Edge edge = new Edge(TaskContext.DEFAULT_EDGE, OperationNames.GATHER);
    inputs.put(parent, edge);

    return this;
  }

  public ComputeConnection gather(String parent, DataType dataType) {
    Edge edge = new Edge(TaskContext.DEFAULT_EDGE, OperationNames.GATHER, dataType);
    inputs.put(parent, edge);

    return this;
  }

  public ComputeConnection gather(String parent, String name) {
    Edge edge = new Edge(name, OperationNames.GATHER, DataType.OBJECT);
    inputs.put(parent, edge);

    return this;
  }

  public ComputeConnection gather(String parent, String name, DataType dataType) {
    Edge edge = new Edge(name, OperationNames.GATHER, dataType);
    inputs.put(parent, edge);

    return this;
  }

  public ComputeConnection partition(String parent) {
    Edge edge = new Edge(TaskContext.DEFAULT_EDGE, OperationNames.PARTITION);
    inputs.put(parent, edge);

    return this;
  }

  public ComputeConnection partition(String parent, DataType dataType) {
    Edge edge = new Edge(TaskContext.DEFAULT_EDGE, OperationNames.PARTITION, dataType);
    inputs.put(parent, edge);

    return this;
  }

  public ComputeConnection partition(String parent, String name) {
    Edge edge = new Edge(name, OperationNames.PARTITION, DataType.OBJECT);
    inputs.put(parent, edge);

    return this;
  }

  public ComputeConnection partition(String parent, String name, DataType dataType) {
    Edge edge = new Edge(name, OperationNames.PARTITION, dataType);
    inputs.put(parent, edge);

    return this;
  }

  public ComputeConnection allreduce(String parent) {
    Edge edge = new Edge(TaskContext.DEFAULT_EDGE, OperationNames.ALLREDUCE);
    inputs.put(parent, edge);

    return this;
  }

  public ComputeConnection allreduce(String parent, DataType dataType) {
    Edge edge = new Edge(TaskContext.DEFAULT_EDGE, OperationNames.ALLREDUCE, dataType);
    inputs.put(parent, edge);

    return this;
  }

  public ComputeConnection allreduce(String parent, String name) {
    Edge edge = new Edge(name, OperationNames.ALLREDUCE, DataType.OBJECT);
    inputs.put(parent, edge);

    return this;
  }

  public ComputeConnection allreduce(String parent, String name, DataType dataType) {
    Edge edge = new Edge(name, OperationNames.ALLREDUCE, dataType);
    inputs.put(parent, edge);

    return this;
  }

  public ComputeConnection allreduce(String parent, String name,
                                     IFunction function, DataType dataType) {
    Edge edge = new Edge(name, OperationNames.ALLREDUCE, dataType, function);
    inputs.put(parent, edge);

    return this;
  }

  public ComputeConnection allreduce(String parent, String name,
                                     Op op, DataType dataType) {
    Edge edge = new Edge(name, OperationNames.ALLREDUCE, dataType, new ReduceFn(op, dataType));
    inputs.put(parent, edge);

    return this;
  }

  public ComputeConnection allgather(String parent) {
    Edge edge = new Edge(TaskContext.DEFAULT_EDGE, OperationNames.ALLGATHER);
    inputs.put(parent, edge);

    return this;
  }

  public ComputeConnection allgather(String parent, DataType dataType) {
    Edge edge = new Edge(TaskContext.DEFAULT_EDGE, OperationNames.ALLGATHER, dataType);
    inputs.put(parent, edge);

    return this;
  }

  public ComputeConnection allgather(String parent, String name) {
    Edge edge = new Edge(name, OperationNames.ALLGATHER, DataType.OBJECT);
    inputs.put(parent, edge);

    return this;
  }

  public ComputeConnection allgather(String parent, String name, DataType dataType) {
    Edge edge = new Edge(name, OperationNames.ALLGATHER, dataType);
    inputs.put(parent, edge);

    return this;
  }

  void build(DataFlowTaskGraph graph) {
    for (Map.Entry<String, Edge> e : inputs.entrySet()) {
      Vertex v1 = graph.vertex(nodeName);
      if (v1 == null) {
        throw new RuntimeException("Failed to connect non-existing task: " + nodeName);
      }

      Vertex v2 = graph.vertex(e.getKey());
      if (v2 == null) {
        throw new RuntimeException("Failed to connect non-existing task: " + e.getKey());
      }
      graph.addTaskEdge(v2, v1, e.getValue());
    }
  }
}
