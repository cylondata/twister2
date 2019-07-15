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
import edu.iu.dsc.tws.api.comms.LogicalPlan;
import edu.iu.dsc.tws.api.comms.SingularReceiver;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.task.IMessage;
import edu.iu.dsc.tws.api.task.TaskMessage;
import edu.iu.dsc.tws.api.task.graph.Edge;
import edu.iu.dsc.tws.comms.selectors.LoadBalanceSelector;
import edu.iu.dsc.tws.comms.stream.SPartition;
import edu.iu.dsc.tws.executor.comms.AbstractParallelOperation;

/**
 * The streaming operation.
 */
public class PartitionStreamingOperation extends AbstractParallelOperation {
  private boolean checkpointStarted = false;

  protected SPartition op;

  public PartitionStreamingOperation(Config config, Communicator network, LogicalPlan tPlan,
                                     Set<Integer> srcs, Set<Integer> dests, Edge edge) {
    super(config, network, tPlan, edge.getName());
    if (srcs.size() == 0) {
      throw new IllegalArgumentException("Sources should have more than 0 elements");
    }

    if (dests == null) {
      throw new IllegalArgumentException("Targets should have more than 0 elements");
    }

    Communicator newComm = channel.newWithConfig(edge.getProperties());
    op = new SPartition(newComm, logicalPlan, srcs, dests,
        edge.getDataType(),
        new PartitionBulkReceiver(),
        new LoadBalanceSelector(), edge.getEdgeID().nextId());
  }

  public boolean send(int source, IMessage message, int flags) {
    return op.partition(source, message.getContent(), flags);
  }

  public class PartitionBulkReceiver implements SingularReceiver {
    @Override
    public void init(Config cfg, Set<Integer> targets) {
    }

    @Override
    public boolean receive(int target, Object data) {
      BlockingQueue<IMessage> messages = outMessages.get(target);

      TaskMessage msg = new TaskMessage<>(data, inEdge, target);
      return messages.offer(msg);
    }
  }

  public boolean progress() {
    return op.progress();
  }

  @Override
  public void close() {
    op.close();
  }

  @Override
  public void reset() {
    op.reset();
  }

  @Override
  public boolean isComplete() {
    return !op.hasPending();
  }
}
