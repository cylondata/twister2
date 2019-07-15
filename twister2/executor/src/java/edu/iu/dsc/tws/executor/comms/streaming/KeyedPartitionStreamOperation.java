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
package edu.iu.dsc.tws.executor.comms.streaming;

import java.util.Set;
import java.util.concurrent.BlockingQueue;

import edu.iu.dsc.tws.api.comms.Communicator;
import edu.iu.dsc.tws.api.comms.DestinationSelector;
import edu.iu.dsc.tws.api.comms.LogicalPlan;
import edu.iu.dsc.tws.api.comms.SingularReceiver;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageType;
import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.task.IMessage;
import edu.iu.dsc.tws.api.task.TaskMessage;
import edu.iu.dsc.tws.api.task.graph.Edge;
import edu.iu.dsc.tws.comms.selectors.HashingSelector;
import edu.iu.dsc.tws.comms.stream.SKeyedPartition;
import edu.iu.dsc.tws.executor.comms.AbstractParallelOperation;
import edu.iu.dsc.tws.executor.comms.DefaultDestinationSelector;

public class KeyedPartitionStreamOperation extends AbstractParallelOperation {
  private SKeyedPartition op;

  public KeyedPartitionStreamOperation(Config config, Communicator network, LogicalPlan tPlan,
                                       Set<Integer> sources, Set<Integer> dests, Edge edge) {
    super(config, network, tPlan, edge.getName());
    MessageType dataType = edge.getDataType();
    MessageType keyType = edge.getKeyType();

    if (sources.size() == 0) {
      throw new IllegalArgumentException("Sources should have more than 0 elements");
    }

    if (dests.size() == 0) {
      throw new IllegalArgumentException("Sources should have more than 0 elements");
    }

    DestinationSelector destSelector;
    if (edge.getPartitioner() != null) {
      destSelector = new DefaultDestinationSelector(edge.getPartitioner());
    } else {
      destSelector = new HashingSelector();
    }

    Communicator newComm = channel.newWithConfig(edge.getProperties());
    op = new SKeyedPartition(newComm, logicalPlan, sources, dests, keyType, dataType,
        new PartitionRecvrImpl(), destSelector, edge.getEdgeID().nextId());
  }

  @Override
  public boolean send(int source, IMessage message, int flags) {
    TaskMessage<Tuple> taskMessage = (TaskMessage) message;
    return op.partition(source,
        taskMessage.getContent().getKey(), taskMessage.getContent().getValue(), flags);
  }

  @Override
  public boolean progress() {
    return op.progress() || op.hasPending();
  }

  private class PartitionRecvrImpl implements SingularReceiver {
    @Override
    public void init(Config cfg, Set<Integer> targets) {

    }

    @Override
    public boolean receive(int target, Object data) {
      if (data instanceof Tuple) {
        TaskMessage msg = new TaskMessage<>(data, inEdge, target);
        BlockingQueue<IMessage> messages = outMessages.get(target);
        if (messages != null) {
          if (messages.offer(msg)) {
            return true;
          }
        }
      } else {
        throw new RuntimeException("Un-expecte data - " + data.getClass());
      }
      return true;
    }
  }

  @Override
  public void close() {
    op.close();
  }

  @Override
  public void reset() {
    op.refresh();
  }

  @Override
  public boolean isComplete() {
    return !op.hasPending();
  }
}
