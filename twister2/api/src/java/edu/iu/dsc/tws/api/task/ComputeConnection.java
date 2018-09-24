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
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.task.function.ReduceFn;
import edu.iu.dsc.tws.comms.api.Op;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.executor.core.OperationNames;
import edu.iu.dsc.tws.task.api.IFunction;
import edu.iu.dsc.tws.task.graph.DataFlowTaskGraph;
import edu.iu.dsc.tws.task.graph.Edge;
import edu.iu.dsc.tws.task.graph.Vertex;

/**
 * Represents a compute connection.
 */
public class ComputeConnection {
  private static final Logger LOG = Logger.getLogger(ComputeConnection.class.getName());

  /**
   * Name of the node, that is trying to connect to other nodes in the graph
   */
  private String nodeName;

  /**
   * The inputs created through this connection
   */
  private Map<String, Edge> inputs = new HashMap<>();

  /**
   * Create a compute connection
   *
   * @param nodeName the name of the node
   */
  ComputeConnection(String nodeName) {
    this.nodeName = nodeName;
  }

  /**
   * Create a broadcast connection
   *
   * @param parent the parent to connection
   * @return the ComputeConnection
   */
  public ComputeConnection broadcast(String parent) {
    Edge edge = new Edge(TaskConfigurations.DEFAULT_EDGE, OperationNames.BROADCAST);
    inputs.put(parent, edge);

    return this;
  }

  /**
   * Create a reduce connection
   *
   * @param parent the parent to connection
   * @param function the reduce function
   * @return the ComputeConnection
   */
  public ComputeConnection reduce(String parent, IFunction function) {
    Edge edge = new Edge(TaskConfigurations.DEFAULT_EDGE, OperationNames.REDUCE, function);
    inputs.put(parent, edge);

    return this;
  }

  /**
   * Create a reduce connection
   *
   * @param parent the parent to connection
   * @param op the reduce function.
   * @param dataType the data type
   * @return the ComputeConnection
   */
  public ComputeConnection reduce(String parent, Op op, DataType dataType) {
    if (!isPrimitiveType(dataType)) {
      LOG.log(Level.SEVERE, "Reduce operations are only applicable to primitive types");
    }

    Edge edge = new Edge(TaskConfigurations.DEFAULT_EDGE, OperationNames.REDUCE,
        dataType, new ReduceFn(op, dataType));
    inputs.put(parent, edge);

    return this;
  }

  /**
   * Create a reduce connection
   *
   * @param parent the parent to connection
   * @param function the reduce function
   * @param dataType the data type
   * @return the ComputeConnection
   */
  public ComputeConnection reduce(String parent, IFunction function, DataType dataType) {
    Edge edge = new Edge(TaskConfigurations.DEFAULT_EDGE,
        OperationNames.REDUCE, dataType, function);
    inputs.put(parent, edge);

    return this;
  }

  /**
   * Create a reduce connection
   *
   * @param parent the parent to connection
   * @param name name of the edge
   * @param function the reduce function
   * @return the ComputeConnection
   */
  public ComputeConnection reduce(String parent, String name, IFunction function) {
    Edge edge = new Edge(name, OperationNames.REDUCE, DataType.OBJECT, function);
    inputs.put(parent, edge);

    return this;
  }

  /**
   * Create a reduce connection
   *
   * @param parent the parent to connection
   * @param name name of the edge
   * @param op the reduce function.
   * @param dataType the data type
   *
   * @return the ComputeConnection
   */
  public ComputeConnection reduce(String parent, String name, Op op, DataType dataType) {
    if (!isPrimitiveType(dataType)) {
      LOG.log(Level.SEVERE, "Reduce operations are only applicable to primitive types");
    }

    Edge edge = new Edge(name, OperationNames.REDUCE, dataType,
        new ReduceFn(op, dataType));
    inputs.put(parent, edge);

    return this;
  }

  /**
   * Create a reduce connection
   *
   * @param parent the parent to connection
   * @param name name of the edge
   * @param function the reduce function
   * @param dataType the data type
   *
   * @return the ComputeConnection
   */
  public ComputeConnection reduce(String parent, String name,
                                  IFunction function, DataType dataType) {
    Edge edge = new Edge(name, OperationNames.REDUCE, dataType, function);
    inputs.put(parent, edge);

    return this;
  }

  /**
   * Create a keyed reduce connection
   *
   * @param parent the parent to connection
   * @param name name of the edge
   * @param function the reduce function
   * @param keyTpe the key data type
   * @param dataType the data type
   *
   * @return the ComputeConnection
   */
  public ComputeConnection keyedReduce(String parent, String name,
                                       IFunction function, DataType keyTpe, DataType dataType) {
    Edge edge = new Edge(name, OperationNames.KEYED_REDUCE, dataType, keyTpe, function);
    inputs.put(parent, edge);

    return this;
  }

  /**
   * Create a gather connection
   *
   * @param parent the parent to connection
   * @return the ComputeConnection
   */
  public ComputeConnection gather(String parent) {
    Edge edge = new Edge(TaskConfigurations.DEFAULT_EDGE, OperationNames.GATHER,
        DataType.OBJECT, DataType.OBJECT);
    inputs.put(parent, edge);

    return this;
  }

  /**
   * Create a gather connection
   *
   * @param parent the parent to connection
   * @param dataType the data type
   * @return the ComputeConnection
   */
  public ComputeConnection gather(String parent, DataType dataType) {
    Edge edge = new Edge(TaskConfigurations.DEFAULT_EDGE, OperationNames.GATHER, dataType);
    inputs.put(parent, edge);

    return this;
  }

  /**
   * Create a gather connection
   *
   * @param parent the parent to connection
   * @param name name of the edge
   * @return the ComputeConnection
   */
  public ComputeConnection gather(String parent, String name) {
    Edge edge = new Edge(name, OperationNames.GATHER, DataType.OBJECT);
    inputs.put(parent, edge);

    return this;
  }

  /**
   * Create a keyed gather connection
   *
   * @param parent the parent to connection
   * @param name name of the edge
   * @param keyTpe the key data type
   * @param dataType the data type
   *
   * @return the ComputeConnection
   */
  public ComputeConnection keyedGather(String parent, String name,
                                       DataType keyTpe, DataType dataType) {
    Edge edge = new Edge(name, OperationNames.KEYED_GATHER, dataType, keyTpe);
    inputs.put(parent, edge);

    return this;
  }

  /**
   * Create a gather connection
   *
   * @param parent the parent to connection
   * @param name name of the edge
   * @param dataType data type
   * @return the ComputeConnection
   */
  public ComputeConnection gather(String parent, String name, DataType dataType) {
    Edge edge = new Edge(name, OperationNames.GATHER, dataType);
    inputs.put(parent, edge);

    return this;
  }


  /**
   * Create a gather connection
   *
   * @param parent the parent to connection
   * @param name name of the edge
   * @param dataType data type
   * @param props map of properties
   *
   * @return the ComputeConnection
   */
  public ComputeConnection gather(String parent, String name, DataType dataType,
                                  Map<String, Object> props) {
    Edge edge = new Edge(name, OperationNames.GATHER, dataType);
    edge.addProperties(props);
    inputs.put(parent, edge);

    return this;
  }

  /**
   * Create a partition connection
   *
   * @param parent the parent to connection
   * @return the ComputeConnection
   */
  public ComputeConnection partition(String parent) {
    Edge edge = new Edge(TaskConfigurations.DEFAULT_EDGE, OperationNames.PARTITION);
    inputs.put(parent, edge);

    return this;
  }

  /**
   * Create a partition connection
   *
   * @param parent the parent to connection
   * @param dataType data type
   * @return the ComputeConnection
   */
  public ComputeConnection partition(String parent, DataType dataType) {
    Edge edge = new Edge(TaskConfigurations.DEFAULT_EDGE, OperationNames.PARTITION, dataType);
    inputs.put(parent, edge);

    return this;
  }

  /**
   * Create a partition connection
   *
   * @param parent the parent to connection
   * @param name name of the edge
   * @return the ComputeConnection
   */
  public ComputeConnection partition(String parent, String name) {
    Edge edge = new Edge(name, OperationNames.PARTITION, DataType.OBJECT);
    inputs.put(parent, edge);

    return this;
  }

  /**
   * Create a partition connection
   *
   * @param parent the parent to connection
   * @param name name of the edge
   * @param dataType data type
   * @return the ComputeConnection
   */
  public ComputeConnection partition(String parent, String name, DataType dataType) {
    Edge edge = new Edge(name, OperationNames.PARTITION, dataType);
    inputs.put(parent, edge);

    return this;
  }

  /**
   * Create a reduce connection
   *
   * @param parent the parent to connection
   * @param function the reduce function
   * @return the ComputeConnection
   */
  public ComputeConnection allreduce(String parent, IFunction function) {
    Edge edge = new Edge(TaskConfigurations.DEFAULT_EDGE, OperationNames.ALLREDUCE, function);
    inputs.put(parent, edge);

    return this;
  }

  /**
   * Create a reduce connection
   *
   * @param parent the parent to connection
   * @param op the reduce function.
   * @param dataType the data type
   * @return the ComputeConnection
   */
  public ComputeConnection allreduce(String parent, Op op, DataType dataType) {
    if (!isPrimitiveType(dataType)) {
      LOG.log(Level.SEVERE, "Reduce operations are only applicable to primitive types");
    }

    Edge edge = new Edge(TaskConfigurations.DEFAULT_EDGE, OperationNames.ALLREDUCE,
        dataType, new ReduceFn(op, dataType));
    inputs.put(parent, edge);

    return this;
  }

  /**
   * Create a reduce connection
   *
   * @param parent the parent to connection
   * @param function the reduce function
   * @param dataType the data type
   * @return the ComputeConnection
   */
  public ComputeConnection allreduce(String parent, IFunction function, DataType dataType) {
    Edge edge = new Edge(TaskConfigurations.DEFAULT_EDGE,
        OperationNames.ALLREDUCE, dataType, function);
    inputs.put(parent, edge);

    return this;
  }

  /**
   * Create a reduce connection
   *
   * @param parent the parent to connection
   * @param name name of the edge
   * @param function the reduce function
   * @return the ComputeConnection
   */
  public ComputeConnection allreduce(String parent, String name, IFunction function) {
    Edge edge = new Edge(name, OperationNames.ALLREDUCE, DataType.OBJECT, function);
    inputs.put(parent, edge);

    return this;
  }

  /**
   * Create a reduce connection
   *
   * @param parent the parent to connection
   * @param name name of the edge
   * @param op the reduce function.
   * @param dataType the data type
   *
   * @return the ComputeConnection
   */
  public ComputeConnection allreduce(String parent, String name, Op op, DataType dataType) {
    if (!isPrimitiveType(dataType)) {
      LOG.log(Level.SEVERE, "Reduce operations are only applicable to primitive types");
    }

    Edge edge = new Edge(name, OperationNames.ALLREDUCE, dataType, new ReduceFn(op, dataType));
    inputs.put(parent, edge);

    return this;
  }

  /**
   * Create a reduce connection
   *
   * @param parent the parent to connection
   * @param name name of the edge
   * @param function the reduce function
   * @param dataType the data type
   *
   * @return the ComputeConnection
   */
  public ComputeConnection allreduce(String parent, String name,
                                     IFunction function, DataType dataType) {
    Edge edge = new Edge(name, OperationNames.ALLREDUCE, dataType, function);
    inputs.put(parent, edge);

    return this;
  }

  /**
   * Create a gather connection
   *
   * @param parent the parent to connection
   * @return the ComputeConnection
   */
  public ComputeConnection allgather(String parent) {
    Edge edge = new Edge(TaskConfigurations.DEFAULT_EDGE, OperationNames.ALLGATHER);
    inputs.put(parent, edge);

    return this;
  }

  /**
   * Create a gather connection
   *
   * @param parent the parent to connection
   * @param dataType data type
   * @return the ComputeConnection
   */
  public ComputeConnection allgather(String parent, DataType dataType) {
    Edge edge = new Edge(TaskConfigurations.DEFAULT_EDGE, OperationNames.ALLGATHER, dataType);
    inputs.put(parent, edge);

    return this;
  }

  /**
   * Create a gather connection
   *
   * @param parent the parent to connection
   * @param name name of the edge
   * @return the ComputeConnection
   */
  public ComputeConnection allgather(String parent, String name) {
    Edge edge = new Edge(name, OperationNames.ALLGATHER, DataType.OBJECT);
    inputs.put(parent, edge);

    return this;
  }

  /**
   * Create a gather connection
   *
   * @param parent the parent to connection
   * @param name name of the edge
   * @param dataType data type
   * @return the ComputeConnection
   */
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

  private boolean isPrimitiveType(DataType dataType) {
    return dataType == DataType.INTEGER || dataType == DataType.DOUBLE || dataType == DataType.LONG
        || dataType == DataType.BYTE || dataType == DataType.SHORT;
  }
}
