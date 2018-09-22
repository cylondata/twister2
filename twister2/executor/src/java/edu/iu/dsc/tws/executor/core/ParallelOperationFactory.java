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

import java.util.Set;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.op.Communicator;
import edu.iu.dsc.tws.executor.api.IParallelOperation;
import edu.iu.dsc.tws.executor.comms.batch.AllGatherBatchOperation;
import edu.iu.dsc.tws.executor.comms.batch.AllReduceBatchOperation;
import edu.iu.dsc.tws.executor.comms.batch.BroadcastBatchOperation;
import edu.iu.dsc.tws.executor.comms.batch.GatherBatchOperation;
import edu.iu.dsc.tws.executor.comms.batch.KeyedGatherBatchOperation;
import edu.iu.dsc.tws.executor.comms.batch.KeyedReduceBatchOperation;
import edu.iu.dsc.tws.executor.comms.batch.PartitionBatchOperation;
import edu.iu.dsc.tws.executor.comms.batch.ReduceBatchOperation;
import edu.iu.dsc.tws.executor.comms.streaming.AllGatherStreamingOperation;
import edu.iu.dsc.tws.executor.comms.streaming.AllReduceStreamingOperation;
import edu.iu.dsc.tws.executor.comms.streaming.BroadcastStreamingOperation;
import edu.iu.dsc.tws.executor.comms.streaming.GatherStreamingOperation;
import edu.iu.dsc.tws.executor.comms.streaming.KeyedPartitionStreamOperation;
import edu.iu.dsc.tws.executor.comms.streaming.KeyedReduceStreamingOperation;
import edu.iu.dsc.tws.executor.comms.streaming.PartitionStreamingOperation;
import edu.iu.dsc.tws.executor.comms.streaming.ReduceStreamingOperation;
import edu.iu.dsc.tws.task.graph.Edge;
import edu.iu.dsc.tws.task.graph.OperationMode;

public class ParallelOperationFactory {
  private Communicator channel;

  private Config config;

  private TaskPlan taskPlan;

  private EdgeGenerator edgeGenerator;

  public ParallelOperationFactory(Config cfg, Communicator network,
                                  TaskPlan plan, EdgeGenerator e) {
    this.channel = network;
    this.config = cfg;
    this.taskPlan = plan;
    this.edgeGenerator = e;
  }

  /**
   * Building the parallel operation based on the batch or streaming tasks. And also
   * the sub cateogories depends on the communication used for each edge in the task graph.
   * ***/
  public IParallelOperation build(Edge edge, Set<Integer> sources, Set<Integer> dests,
                                  OperationMode operationMode) {

    if (operationMode.equals(OperationMode.BATCH)) {
      //LOG.info("Batch Job Building ...");
      if (!edge.isKeyed()) {
        if (OperationNames.PARTITION.equals(edge.getOperation())) {
          PartitionBatchOperation partitionOp
              = new PartitionBatchOperation(config, channel, taskPlan);
          partitionOp.prepare(sources, dests, edgeGenerator, edge.getDataType(), edge.getName());
          return partitionOp;
        } else if (OperationNames.BROADCAST.equals(edge.getOperation())) {
          BroadcastBatchOperation bcastOp = new BroadcastBatchOperation(config, channel, taskPlan);
          // get the first as the source
          bcastOp.prepare(sources.iterator().next(), dests, edgeGenerator, edge.getDataType(),
              edge.getName());
          return bcastOp;
        } else if (OperationNames.GATHER.equals(edge.getOperation())) {
          Object shuffleProp = edge.getProperty("shuffle");
          boolean shuffle = false;
          if (shuffleProp != null && shuffleProp instanceof Boolean && (Boolean) shuffleProp) {
            shuffle = true;
          }
          return new GatherBatchOperation(config, channel, taskPlan,
              sources, dests.iterator().next(), edgeGenerator, edge.getDataType(),
              edge.getName(), shuffle);
        } else if (OperationNames.ALLGATHER.equals(edge.getOperation())) {
          return new AllGatherBatchOperation(config, channel, taskPlan,
              sources, dests, edgeGenerator, edge.getDataType(),
              edge.getName());
        } else if (OperationNames.REDUCE.equals(edge.getOperation())) {
          ReduceBatchOperation reduceBatchOperation = new ReduceBatchOperation(config, channel,
              taskPlan);
          reduceBatchOperation.prepare(sources, dests.iterator().next(), edgeGenerator,
              edge.getDataType(), edge.getFunction(), edge.getName());
          return reduceBatchOperation;
        } else if (OperationNames.ALLREDUCE.equals(edge.getOperation())) {
          return new AllReduceBatchOperation(config,
              channel, taskPlan, sources, dests, edgeGenerator, edge.getDataType(),
              edge.getName(), edge.getFunction());
        }
      } else {
        if (OperationNames.KEYED_REDUCE.equals(edge.getOperation())) {
          return new KeyedReduceBatchOperation(config, channel, taskPlan, sources,
              dests, edgeGenerator, edge.getDataType(), edge.getKeyType(),
              edge.getName(), edge.getFunction());
        } else if (OperationNames.KEYED_GATHER.equals(edge.getOperation())) {
          return new KeyedGatherBatchOperation(config, channel, taskPlan, sources,
              dests, edgeGenerator, edge.getDataType(), edge.getKeyType(), edge.getName());
        }  else if (OperationNames.KEYED_PARTITION.equals(edge.getOperation())) {
          return new KeyedGatherBatchOperation(config, channel, taskPlan, sources,
              dests, edgeGenerator, edge.getDataType(), edge.getKeyType(), edge.getName());
        }
      }
    } else if (operationMode.equals(OperationMode.STREAMING)) {
      //LOG.info("Streaming Job Building ...");
      if (!edge.isKeyed()) {
        if (OperationNames.PARTITION.equals(edge.getOperation())) {
          return new PartitionStreamingOperation(config, channel,
              taskPlan, sources, dests, edgeGenerator, edge.getDataType(), edge.getName());
        } else if (OperationNames.BROADCAST.equals(edge.getOperation())) {
          return new BroadcastStreamingOperation(config, channel,
              taskPlan, sources.iterator().next(), dests, edgeGenerator, edge.getDataType(),
              edge.getName());
        } else if (OperationNames.GATHER.equals(edge.getOperation())) {
          return new GatherStreamingOperation(config, channel,
              taskPlan, sources, dests.iterator().next(), edgeGenerator, edge.getDataType(),
              edge.getName(), taskPlan);
        } else if (OperationNames.REDUCE.equals(edge.getOperation())) {
          return new ReduceStreamingOperation(config,
              channel, taskPlan, edge.getFunction(), sources,
              dests.iterator().next(), edgeGenerator, edge.getDataType(), edge.getName());
        } else if (OperationNames.ALLREDUCE.equals(edge.getOperation())) {
          return new AllReduceStreamingOperation(
              config, channel, taskPlan, edge.getFunction(),
              sources, dests, edgeGenerator, edge.getDataType(),
              edge.getName());
        } else if (OperationNames.ALLGATHER.equals(edge.getOperation())) {
          return new AllGatherStreamingOperation(
              config, channel, taskPlan, sources, dests, edgeGenerator, edge.getDataType(),
              edge.getName());
        }
      } else {
        if (OperationNames.KEYED_REDUCE.equals(edge.getOperation())) {
          return new KeyedReduceStreamingOperation(
              config, channel, taskPlan, sources, dests,
              edgeGenerator, edge.getDataType(), edge.getKeyType(),
              edge.getName(), edge.getFunction());
        } else if (OperationNames.KEYED_PARTITION.equals(edge.getOperation())) {
          return new KeyedPartitionStreamOperation(config, channel, taskPlan, sources, dests,
          edgeGenerator, edge.getDataType(), edge.getKeyType(),
          edge.getName());
        }
      }
    }

    throw new RuntimeException("Un-supported operation: " + edge.getOperation());
  }
}
