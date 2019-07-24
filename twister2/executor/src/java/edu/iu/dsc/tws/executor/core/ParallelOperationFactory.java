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
package edu.iu.dsc.tws.executor.core;

import java.util.List;
import java.util.Set;

import edu.iu.dsc.tws.api.comms.Communicator;
import edu.iu.dsc.tws.api.comms.LogicalPlan;
import edu.iu.dsc.tws.api.comms.OperationNames;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.task.executor.IParallelOperation;
import edu.iu.dsc.tws.api.task.graph.Edge;
import edu.iu.dsc.tws.api.task.graph.OperationMode;
import edu.iu.dsc.tws.executor.comms.batch.AllGatherBatchOperation;
import edu.iu.dsc.tws.executor.comms.batch.AllReduceBatchOperation;
import edu.iu.dsc.tws.executor.comms.batch.BroadcastBatchOperation;
import edu.iu.dsc.tws.executor.comms.batch.DirectBatchOperation;
import edu.iu.dsc.tws.executor.comms.batch.GatherBatchOperation;
import edu.iu.dsc.tws.executor.comms.batch.JoinBatchOperation;
import edu.iu.dsc.tws.executor.comms.batch.KeyedGatherBatchOperation;
import edu.iu.dsc.tws.executor.comms.batch.KeyedPartitionBatchOperation;
import edu.iu.dsc.tws.executor.comms.batch.KeyedReduceBatchOperation;
import edu.iu.dsc.tws.executor.comms.batch.PartitionBatchOperation;
import edu.iu.dsc.tws.executor.comms.batch.ReduceBatchOperation;
import edu.iu.dsc.tws.executor.comms.streaming.AllGatherStreamingOperation;
import edu.iu.dsc.tws.executor.comms.streaming.AllReduceStreamingOperation;
import edu.iu.dsc.tws.executor.comms.streaming.BroadcastStreamingOperation;
import edu.iu.dsc.tws.executor.comms.streaming.DirectStreamingOperation;
import edu.iu.dsc.tws.executor.comms.streaming.GatherStreamingOperation;
import edu.iu.dsc.tws.executor.comms.streaming.KeyedGatherStreamingOperation;
import edu.iu.dsc.tws.executor.comms.streaming.KeyedPartitionStreamOperation;
import edu.iu.dsc.tws.executor.comms.streaming.KeyedReduceStreamingOperation;
import edu.iu.dsc.tws.executor.comms.streaming.PartitionStreamingOperation;
import edu.iu.dsc.tws.executor.comms.streaming.ReduceStreamingOperation;

public class ParallelOperationFactory {
  private Communicator channel;

  private Config config;

  private LogicalPlan logicalPlan;

  public ParallelOperationFactory(Config cfg, Communicator network, LogicalPlan plan) {
    this.channel = network;
    this.config = cfg;
    this.logicalPlan = plan;
  }

  public IParallelOperation build(List<Edge> edges, Set<Integer> sources,
                                        Set<Integer> dests, OperationMode operationMode) {
    if (operationMode.equals(OperationMode.BATCH)) {
      if (edges.size() < 1) {
        throw new RuntimeException("Two edges should be present");
      }
      Edge leftEdge = edges.get(0);
      Edge rightEdge = edges.get(1);
      if (leftEdge.isKeyed() && rightEdge.isKeyed()) {
        if (OperationNames.JOIN.equals(leftEdge.getOperation())) {
          return new JoinBatchOperation(config, channel, logicalPlan, sources,
              dests, leftEdge, rightEdge);
        }
      }
    }
    throw new RuntimeException("Un-supported operation name");
  }

  /**
   * Building the parallel operation based on the batch or streaming tasks. And also
   * the sub cateogories depends on the communication used for each edge in the task graph.
   */
  public IParallelOperation build(Edge edge, Set<Integer> sources, Set<Integer> dests,
                                  OperationMode operationMode) {

    if (operationMode.equals(OperationMode.BATCH)) {
      if (!edge.isKeyed()) {
        if (OperationNames.PARTITION.equals(edge.getOperation())) {
          return new PartitionBatchOperation(config, channel, logicalPlan, sources, dests, edge);
        } else if (OperationNames.BROADCAST.equals(edge.getOperation())) {
          return new BroadcastBatchOperation(config, channel, logicalPlan,
              sources, dests, edge);
        } else if (OperationNames.GATHER.equals(edge.getOperation())) {
          return new GatherBatchOperation(config, channel, logicalPlan,
              sources, dests, edge);
        } else if (OperationNames.ALLGATHER.equals(edge.getOperation())) {
          return new AllGatherBatchOperation(config, channel, logicalPlan,
              sources, dests, edge);
        } else if (OperationNames.REDUCE.equals(edge.getOperation())) {
          return new ReduceBatchOperation(config, channel,
              logicalPlan, sources, dests, edge);
        } else if (OperationNames.ALLREDUCE.equals(edge.getOperation())) {
          return new AllReduceBatchOperation(config,
              channel, logicalPlan, sources, dests, edge);
        } else if (OperationNames.DIRECT.equals(edge.getOperation())) {
          return new DirectBatchOperation(config, channel, logicalPlan, sources, dests, edge);
        }
      } else {
        if (OperationNames.KEYED_REDUCE.equals(edge.getOperation())) {
          return new KeyedReduceBatchOperation(config, channel, logicalPlan, sources,
              dests, edge);
        } else if (OperationNames.KEYED_GATHER.equals(edge.getOperation())) {
          return new KeyedGatherBatchOperation(config, channel, logicalPlan, sources,
              dests, edge);
        } else if (OperationNames.KEYED_PARTITION.equals(edge.getOperation())) {
          return new KeyedPartitionBatchOperation(config, channel, logicalPlan, sources,
              dests, edge);
        }
      }
    } else if (operationMode.equals(OperationMode.STREAMING)) {
      if (!edge.isKeyed()) {
        if (OperationNames.PARTITION.equals(edge.getOperation())) {
          return new PartitionStreamingOperation(config, channel,
              logicalPlan, sources, dests, edge);
        } else if (OperationNames.BROADCAST.equals(edge.getOperation())) {
          return new BroadcastStreamingOperation(config, channel,
              logicalPlan, sources, dests, edge);
        } else if (OperationNames.GATHER.equals(edge.getOperation())) {
          return new GatherStreamingOperation(config, channel,
              logicalPlan, sources, dests, edge);
        } else if (OperationNames.REDUCE.equals(edge.getOperation())) {
          return new ReduceStreamingOperation(config,
              channel, logicalPlan, edge.getFunction(), sources,
              dests, edge);
        } else if (OperationNames.ALLREDUCE.equals(edge.getOperation())) {
          return new AllReduceStreamingOperation(
              config, channel, logicalPlan, edge.getFunction(),
              sources, dests, edge);
        } else if (OperationNames.ALLGATHER.equals(edge.getOperation())) {
          return new AllGatherStreamingOperation(
              config, channel, logicalPlan, sources, dests, edge);
        } else if (OperationNames.DIRECT.equals(edge.getOperation())) {
          return new DirectStreamingOperation(config, channel, logicalPlan, sources, dests, edge);
        }
      } else {
        if (OperationNames.KEYED_REDUCE.equals(edge.getOperation())) {
          return new KeyedReduceStreamingOperation(
              config, channel, logicalPlan, sources, dests, edge);
        } else if (OperationNames.KEYED_GATHER.equals(edge.getOperation())) {
          return new KeyedGatherStreamingOperation(config, channel, logicalPlan,
              sources, dests, edge);
        } else if (OperationNames.KEYED_PARTITION.equals(edge.getOperation())) {
          return new KeyedPartitionStreamOperation(config, channel, logicalPlan,
              sources, dests, edge);
        }
      }
    }

    throw new RuntimeException("Un-supported operation: " + edge.getOperation());
  }
}
