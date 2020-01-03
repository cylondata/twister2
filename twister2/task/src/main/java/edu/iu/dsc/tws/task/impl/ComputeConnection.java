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
package edu.iu.dsc.tws.task.impl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import edu.iu.dsc.tws.api.comms.CommunicationContext;
import edu.iu.dsc.tws.api.compute.graph.ComputeGraph;
import edu.iu.dsc.tws.api.compute.graph.Edge;
import edu.iu.dsc.tws.api.compute.graph.Vertex;
import edu.iu.dsc.tws.task.impl.ops.AbstractOpsConfig;
import edu.iu.dsc.tws.task.impl.ops.AllGatherConfig;
import edu.iu.dsc.tws.task.impl.ops.AllReduceConfig;
import edu.iu.dsc.tws.task.impl.ops.BroadcastConfig;
import edu.iu.dsc.tws.task.impl.ops.DirectConfig;
import edu.iu.dsc.tws.task.impl.ops.GatherConfig;
import edu.iu.dsc.tws.task.impl.ops.JoinConfig;
import edu.iu.dsc.tws.task.impl.ops.KeyedGatherConfig;
import edu.iu.dsc.tws.task.impl.ops.KeyedPartitionConfig;
import edu.iu.dsc.tws.task.impl.ops.KeyedReduceConfig;
import edu.iu.dsc.tws.task.impl.ops.PartitionConfig;
import edu.iu.dsc.tws.task.impl.ops.ReduceConfig;

/**
 * Represents a compute connection.
 */
public class ComputeConnection {

  /**
   * Name of the node, that is trying to connect to other nodes in the graph
   */
  private String nodeName;

  /**
   * The inputs created through this connection
   * <Source,<EdgeName,Edge>>
   */
  private Map<String, Map<String, Edge>> inputs = new HashMap<>();

  /**
   * When building up the operation chain, if user don't call {@link AbstractOpsConfig::connect}
   * they will be kept in this map to auto connect later.
   */
  private Map<String, Set<AbstractOpsConfig>> autoConnectConfig = new HashMap<>();

  /**
   * Create a compute connection
   *
   * @param nodeName the name of the node
   */
  ComputeConnection(String nodeName) {
    this.nodeName = nodeName;
  }

  void putEdgeFromSource(String source, Edge edge) {
    Map<String, Edge> edgesFromSource = inputs.computeIfAbsent(source, s -> new HashMap<>());
    if (edgesFromSource.containsKey(edge.getName())) {
      throw new RuntimeException("Edges from the same source should be unique. "
          + "Found " + edge.getName() + " already defined from source " + source);
    }
    edgesFromSource.put(edge.getName(), edge);
  }

  private void addToAutoConfig(String source, AbstractOpsConfig config) {
    this.autoConnectConfig.computeIfAbsent(source, s -> new HashSet<>()).add(config);
  }

  /**
   * Create a broadcast configuration
   *
   * @param source the source to connection
   * @return the {@link BroadcastConfig}
   */
  public BroadcastConfig broadcast(String source) {
    BroadcastConfig broadcastConfig = new BroadcastConfig(source, this);
    this.addToAutoConfig(source, broadcastConfig);
    return broadcastConfig;
  }

  /**
   * Create a reduce configuration
   *
   * @param source the source to connection
   * @return the {@link ReduceConfig}
   */
  public ReduceConfig reduce(String source) {
    ReduceConfig reduceConfig = new ReduceConfig(source, this);
    this.addToAutoConfig(source, reduceConfig);
    return reduceConfig;
  }

  /**
   * Create a keyed reduce config
   *
   * @param source the source to connection
   * @return the {@link KeyedReduceConfig}
   */
  public KeyedReduceConfig keyedReduce(String source) {
    KeyedReduceConfig keyedReduceConfig = new KeyedReduceConfig(source, this);
    this.addToAutoConfig(source, keyedReduceConfig);
    return keyedReduceConfig;
  }

  /**
   * Create a gather config
   *
   * @param source the source to connection
   * @return the {@link GatherConfig}
   */
  public GatherConfig gather(String source) {
    GatherConfig gatherConfig = new GatherConfig(source, this);
    this.addToAutoConfig(source, gatherConfig);
    return gatherConfig;
  }

  /**
   * Create a keyed gather config
   *
   * @param source the source to connection
   * @return the {@link KeyedGatherConfig}
   */
  public KeyedGatherConfig keyedGather(String source) {
    KeyedGatherConfig keyedGatherConfig = new KeyedGatherConfig(source, this);
    this.addToAutoConfig(source, keyedGatherConfig);
    return keyedGatherConfig;
  }

  /**
   * Create an inner join operation. The left source and right source can be the same source
   *
   * @param leftSource the left source to connection
   * @param rightSource the right source to connection
   * @return the {@link JoinConfig}
   */
  public JoinConfig innerJoin(String leftSource, String rightSource,
                              CommunicationContext.JoinAlgorithm joinAlgorithm) {
    JoinConfig joinConfig = new JoinConfig(leftSource, rightSource,
        this, CommunicationContext.JoinType.INNER, joinAlgorithm);
    this.addToAutoConfig(leftSource, joinConfig);
    return joinConfig;
  }

  /**
   * Create an full outer join operation. The left source and right source can be the same source
   *
   * @param leftSource the left source to connection
   * @param rightSource the right source to connection
   * @return the {@link JoinConfig}
   */
  public JoinConfig fullOuterJoin(String leftSource, String rightSource) {
    JoinConfig joinConfig = new JoinConfig(leftSource, rightSource,
        this, CommunicationContext.JoinType.FULL_OUTER,
        CommunicationContext.JoinAlgorithm.SORT);
    this.addToAutoConfig(leftSource, joinConfig);
    return joinConfig;
  }

  /**
   * Create an left outer join operation. The left source and right source can be the same source
   *
   * @param leftSource the left source to connection
   * @param rightSource the right source to connection
   * @return the {@link JoinConfig}
   */
  public JoinConfig leftOuterJoin(String leftSource, String rightSource,
                                  CommunicationContext.JoinAlgorithm joinAlgorithm) {
    JoinConfig joinConfig = new JoinConfig(leftSource, rightSource,
        this, CommunicationContext.JoinType.LEFT, joinAlgorithm);
    this.addToAutoConfig(leftSource, joinConfig);
    return joinConfig;
  }

  /**
   * Create an right outer join operation. The left source and right source can be the same source
   *
   * @param leftSource the left source to connection
   * @param rightSource the right source to connection
   * @return the {@link JoinConfig}
   */
  public JoinConfig rightOuterJoin(String leftSource, String rightSource,
                                   CommunicationContext.JoinAlgorithm joinAlgorithm) {
    JoinConfig joinConfig = new JoinConfig(leftSource, rightSource,
        this, CommunicationContext.JoinType.RIGHT, joinAlgorithm);
    this.addToAutoConfig(leftSource, joinConfig);
    return joinConfig;
  }


  /**
   * Create a partition config
   *
   * @param source the source to connection
   * @return the {@link KeyedGatherConfig}
   */
  public PartitionConfig partition(String source) {
    PartitionConfig partitionConfig = new PartitionConfig(source, this);
    this.addToAutoConfig(source, partitionConfig);
    return partitionConfig;
  }

  /**
   * Create a keyed partition config
   *
   * @param source the source to connection
   * @return the {@link KeyedPartitionConfig}
   */
  public KeyedPartitionConfig keyedPartition(String source) {
    KeyedPartitionConfig keyedPartitionConfig = new KeyedPartitionConfig(source, this);
    this.addToAutoConfig(source, keyedPartitionConfig);
    return keyedPartitionConfig;
  }

  /**
   * Create an allreduce config
   *
   * @param source the source to connection
   * @return the {@link AllReduceConfig}
   */
  public AllReduceConfig allreduce(String source) {
    AllReduceConfig allReduceConfig = new AllReduceConfig(source, this);
    this.addToAutoConfig(source, allReduceConfig);
    return allReduceConfig;
  }


  /**
   * Create an allgather config
   *
   * @param source the source to connection
   * @return the {@link AllGatherConfig}
   */
  public AllGatherConfig allgather(String source) {
    AllGatherConfig allGatherConfig = new AllGatherConfig(source, this);
    this.addToAutoConfig(source, allGatherConfig);
    return allGatherConfig;
  }


  /**
   * Crate a direct config
   *
   * @param source the source to connection
   * @return the {@link DirectConfig}
   */
  public DirectConfig direct(String source) {
    DirectConfig directConfig = new DirectConfig(source, this);
    this.addToAutoConfig(source, directConfig);
    return directConfig;
  }

  private void doAutoConnect() {
    this.autoConnectConfig.forEach((source, configs) -> {
      configs.forEach(abstractOpsConfig -> {
        if (!(this.inputs.containsKey(source)
            && this.inputs.get(source).containsKey(abstractOpsConfig.getEdgeName()))) {
          abstractOpsConfig.connect();
        }
      });
    });
    this.autoConnectConfig.clear();
  }

  void build(ComputeGraph graph) {
    this.doAutoConnect();
    this.inputs.forEach((source, edges) -> {
      edges.forEach((edgeName, edge) -> {
        Vertex v1 = graph.vertex(nodeName);
        if (v1 == null) {
          throw new RuntimeException("Failed to connect non-existing task: " + nodeName);
        }

        Vertex v2 = graph.vertex(source);
        if (v2 == null) {
          throw new RuntimeException("Failed to connect non-existing task: " + source);
        }
        graph.addTaskEdge(v2, v1, edge);
      });
    });
  }
}
