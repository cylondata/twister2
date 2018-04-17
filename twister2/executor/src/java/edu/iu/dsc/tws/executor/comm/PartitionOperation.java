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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.api.TWSChannel;
import edu.iu.dsc.tws.comms.core.TWSNetwork;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.mpi.MPIDataFlowPartition;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.TaskMessage;

public class PartitionOperation {
  private MPIDataFlowPartition partition;

  private Config config;

  private TWSNetwork network;

  private TWSChannel channel;

  private BlockingQueue<IMessage> outMessages;

  private MPIDataFlowPartition op;

  private TaskPlan taskPlan;

  private int edge;

  public PartitionOperation(Config config, TWSNetwork network, TaskPlan tPlan,
                            BlockingQueue<IMessage> outMsgs) {
    this.config = config;
    this.network = network;
    this.taskPlan = tPlan;
    this.outMessages = outMsgs;
  }

  public void prepare(Set<Integer> srcs, Set<Integer> dests, int e,
                      DataType dataType, DataType keyType) {
    this.edge = e;
    op = new MPIDataFlowPartition(channel, srcs, dests, new PartitionReceiver(),
        MPIDataFlowPartition.PartitionStratergy.DIRECT,
        Utils.dataTypeToMessageType(dataType), Utils.dataTypeToMessageType(keyType));
    op.init(config, Utils.dataTypeToMessageType(dataType), taskPlan, e);
  }

  public void send(int source, IMessage message) {
    op.send(source, message, 0);
  }

  public void send(int source, IMessage message, int dest) {
    op.send(source, message, 0, dest);
  }

  public class PartitionReceiver implements MessageReceiver {
    @Override
    public void init(Config cfg, DataFlowOperation operation,
                     Map<Integer, List<Integer>> expectedIds) {
    }

    @Override
    public boolean onMessage(int source, int path, int target, int flags, Object object) {
      TaskMessage<Object> msg = new TaskMessage<>(object, edge, source);
      return outMessages.offer(msg);
    }

    @Override
    public void progress() {
    }
  }
}
