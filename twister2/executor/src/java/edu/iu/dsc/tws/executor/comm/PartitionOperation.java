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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.api.TWSChannel;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.mpi.MPIDataFlowPartition;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.executor.EdgeGenerator;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.TaskMessage;

public class PartitionOperation extends ParallelOperation {
  private static final Logger LOG = Logger.getLogger(PartitionOperation.class.getName());

  private Config config;

  private TWSChannel channel;

  private Map<Integer, BlockingQueue<IMessage>> outMessages;

  private MPIDataFlowPartition op;

  private TaskPlan taskPlan;

  private EdgeGenerator edge;

  private int partitionEdge;

  public PartitionOperation(Config config, TWSChannel network, TaskPlan tPlan) {
    this.config = config;
    this.taskPlan = tPlan;
    this.channel = network;
    this.outMessages = new HashMap<>();
  }

  public void prepare(Set<Integer> srcs, Set<Integer> dests, EdgeGenerator e,
                      DataType dataType, String edgeName) {
    this.edge = e;
    op = new MPIDataFlowPartition(channel, srcs, dests, new PartitionReceiver(),
        MPIDataFlowPartition.PartitionStratergy.DIRECT);
    partitionEdge = e.generate(edgeName);
    op.init(config, Utils.dataTypeToMessageType(dataType), taskPlan, partitionEdge);
  }

  public void prepare(Set<Integer> srcs, Set<Integer> dests, EdgeGenerator e,
                      DataType dataType, DataType keyType, String edgeName) {
    this.edge = e;
    op = new MPIDataFlowPartition(channel, srcs, dests, new PartitionReceiver(),
        MPIDataFlowPartition.PartitionStratergy.DIRECT,
        Utils.dataTypeToMessageType(dataType), Utils.dataTypeToMessageType(keyType));
    partitionEdge = e.generate(edgeName);
    op.init(config, Utils.dataTypeToMessageType(dataType), taskPlan, partitionEdge);
  }

  public void send(int source, IMessage message) {
    op.send(source, message, 0);
  }

  public void send(int source, IMessage message, int dest) {
    op.send(source, message, 0, dest);
  }

  @Override
  public void register(int targetTask, BlockingQueue<IMessage> queue) {
    if (outMessages.containsKey(targetTask)) {
      throw new RuntimeException("Existing queue for target task");
    }
    outMessages.put(targetTask, queue);
  }

  public class PartitionReceiver implements MessageReceiver {
    @Override
    public void init(Config cfg, DataFlowOperation operation,
                     Map<Integer, List<Integer>> expectedIds) {
    }

    @Override
    public boolean onMessage(int source, int path, int target, int flags, Object object) {
      TaskMessage<Object> msg = new TaskMessage<>(object,
          edge.getStringMapping(partitionEdge), target);
      return outMessages.get(target).offer(msg);
    }

    @Override
    public void progress() {
    }
  }
}
