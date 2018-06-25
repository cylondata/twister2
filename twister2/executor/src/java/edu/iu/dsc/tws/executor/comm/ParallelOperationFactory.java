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
package edu.iu.dsc.tws.executor.comm;

import java.util.Set;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.TWSChannel;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.executor.EdgeGenerator;
import edu.iu.dsc.tws.task.api.Operations;
import edu.iu.dsc.tws.task.graph.Edge;

public class ParallelOperationFactory {
  private TWSChannel channel;

  private Config config;

  private TaskPlan taskPlan;

  private EdgeGenerator edgeGenerator;

  public ParallelOperationFactory(Config cfg, TWSChannel network, TaskPlan plan, EdgeGenerator e) {
    this.channel = network;
    this.config = cfg;
    this.taskPlan = plan;
    this.edgeGenerator = e;
  }

  /**
   * Factory Format to Choose the Corresponding Operation
   */
  public IParallelOperation build(Edge edge, Set<Integer> sources, Set<Integer> dests) {
    if (!edge.isKeyed()) {
      if (Operations.PARTITION.equals(edge.getOperation())) {
        PartitionOperation partitionOp = new PartitionOperation(config, channel, taskPlan);
        partitionOp.prepare(sources, dests, edgeGenerator, edge.getDataType(), edge.getName());
        return partitionOp;
      } else if (Operations.BROADCAST.equals(edge.getOperation())) {
        BroadcastOperation bcastOp = new BroadcastOperation(config, channel, taskPlan);
        // get the first as the source
        bcastOp.prepare(sources.iterator().next(), dests, edgeGenerator, edge.getDataType(),
            edge.getName());
        return bcastOp;
      } else if (Operations.GATHER.equals(edge.getOperation())) {
        GatherOperation gatherOp = new GatherOperation(config, channel, taskPlan);
        gatherOp.prepare(sources, dests.iterator().next(), edgeGenerator, edge.getDataType(),
            edge.getName(), config, taskPlan);
        return gatherOp;
      } else if (Operations.PARTITION_BY_MULTI_BYTE.equals(edge.getOperation())) {
        PartitionByMultiByteOperation partitionByMultiByteOperation
            = new PartitionByMultiByteOperation(config, channel, taskPlan);
        partitionByMultiByteOperation.prepare(sources, dests, edgeGenerator, edge.getDataType(),
            edge.getName());
        return partitionByMultiByteOperation;
      } else if (Operations.REDUCE.equals(edge.getOperation())) {
        ReduceOperation reduceOperation = new ReduceOperation(config, channel, taskPlan);
        reduceOperation.prepare(sources, dests.iterator().next(), edgeGenerator,
            edge.getDataType(), edge.getName());
        return reduceOperation;
      } else if (Operations.ALL_REDUCE.equals(edge.getOperation())) {
        AllReduceOperation allReduceOperation = new AllReduceOperation(config, channel, taskPlan);
        allReduceOperation.prepare(sources, dests, edgeGenerator, edge.getDataType(),
            edge.getName());
        return allReduceOperation;
      } else if (Operations.KEYED_REDUCE.equals(edge.getOperation())) {
        KeyedReduceOperation keyedReduceOperation
            = new KeyedReduceOperation(config, channel, taskPlan);
        keyedReduceOperation.prepare(sources, dests, edgeGenerator, edge.getDataType(),
            edge.getName());
        return keyedReduceOperation;
      }
    } else {
      if (Operations.PARTITION.equals(edge.getOperation())) {
        PartitionOperation partitionOp = new PartitionOperation(config, channel, taskPlan);
        partitionOp.prepare(sources, dests, edgeGenerator, edge.getDataType(),
            edge.getKeyType(), edge.getName());
        return partitionOp;
      } else if (Operations.BROADCAST.equals(edge.getOperation())) {
        BroadcastOperation broadcastOp = new BroadcastOperation(config, channel, taskPlan);
        broadcastOp.prepare(sources.iterator().next(), dests, edgeGenerator, edge.getDataType(),
            edge.getName());
        return broadcastOp;
      } else if (Operations.GATHER.equals(edge.getOperation())) {
        GatherOperation gatherOp = new GatherOperation(config, channel, taskPlan);
        gatherOp.prepare(sources, dests.iterator().next(), edgeGenerator, edge.getDataType(),
            edge.getKeyType(), edge.getName(), config, taskPlan);
        return gatherOp;
      } else if (Operations.PARTITION_BY_MULTI_BYTE.equals(edge.getOperation())) {
        PartitionByMultiByteOperation partitionByMultiByteOperation
            = new PartitionByMultiByteOperation(config, channel, taskPlan);
        partitionByMultiByteOperation.prepare(sources, dests, edgeGenerator, edge.getDataType(),
            edge.getKeyType(), edge.getName());
        return partitionByMultiByteOperation;
      } else if (Operations.REDUCE.equals(edge.getOperation())) {
        ReduceOperation reduceOperation = new ReduceOperation(config, channel, taskPlan);
        reduceOperation.prepare(sources, dests.iterator().next(), edgeGenerator,
            edge.getDataType(), edge.getName());
        return reduceOperation;
      } else if (Operations.ALL_REDUCE.equals(edge.getOperation())) {
        AllReduceOperation allReduceOperation = new AllReduceOperation(config, channel, taskPlan);
        allReduceOperation.prepare(sources, dests, edgeGenerator, edge.getDataType(),
            edge.getName());
        return allReduceOperation;
      } else if (Operations.KEYED_REDUCE.equals(edge.getOperation())) {
        KeyedReduceOperation keyedReduceOperation
            = new KeyedReduceOperation(config, channel, taskPlan);
        keyedReduceOperation.prepare(sources, dests, edgeGenerator, edge.getDataType(),
            edge.getName());
        return keyedReduceOperation;
      }
    }
    return null;
  }

  public IParallelOperation build(Edge edge, Set<Integer> sources, Set<Integer> dests,
                                  DataType dataType, DataType keyType) {
    if (Operations.PARTITION.equals(edge.getOperation())) {
      PartitionOperation partitionOp = new PartitionOperation(config, channel, taskPlan);
      partitionOp.prepare(sources, dests, edgeGenerator, dataType, keyType, edge.getName());
      return partitionOp;
    }
    return null;
  }

}
