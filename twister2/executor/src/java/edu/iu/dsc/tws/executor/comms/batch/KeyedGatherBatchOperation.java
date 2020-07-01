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
package edu.iu.dsc.tws.executor.comms.batch;

import java.util.Comparator;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

import edu.iu.dsc.tws.api.comms.BaseOperation;
import edu.iu.dsc.tws.api.comms.BulkReceiver;
import edu.iu.dsc.tws.api.comms.CommunicationContext;
import edu.iu.dsc.tws.api.comms.Communicator;
import edu.iu.dsc.tws.api.comms.DestinationSelector;
import edu.iu.dsc.tws.api.comms.LogicalPlan;
import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.compute.IMessage;
import edu.iu.dsc.tws.api.compute.TaskMessage;
import edu.iu.dsc.tws.api.compute.graph.Edge;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.comms.batch.BKeyedGather;
import edu.iu.dsc.tws.comms.selectors.HashingSelector;
import edu.iu.dsc.tws.executor.comms.AbstractParallelOperation;
import edu.iu.dsc.tws.executor.comms.DefaultDestinationSelector;

public class KeyedGatherBatchOperation extends AbstractParallelOperation {

  protected BKeyedGather op;

  public KeyedGatherBatchOperation(Config config, Communicator network, LogicalPlan tPlan,
                                   Set<Integer> sources, Set<Integer> dests, Edge edge,
                                   Map<Integer, Integer> srcGlobalToIndex,
                                   Map<Integer, Integer> tgtsGlobalToIndex) {
    super(config, network, tPlan, edge.getName());

    DestinationSelector destSelector;
    if (edge.getPartitioner() != null) {
      destSelector = new DefaultDestinationSelector(edge.getPartitioner(),
          srcGlobalToIndex, tgtsGlobalToIndex);
    } else {
      destSelector = new HashingSelector();
    }

    boolean useDisk = false;
    Comparator keyComparator = null;
    boolean groupByKey = true;
    try {
      groupByKey = (Boolean) edge.getProperty(CommunicationContext.GROUP_BY_KEY);
      useDisk = (Boolean) edge.getProperty(CommunicationContext.USE_DISK);
      keyComparator = (Comparator) edge.getProperty(CommunicationContext.KEY_COMPARATOR);
    } catch (Exception ex) {
      //ignore
    }

    Communicator newComm = channel.newWithConfig(edge.getProperties());
    op = new BKeyedGather(newComm, logicalPlan, sources, dests,
        edge.getKeyType(), edge.getDataType(), new GatherRecvrImpl(),
        destSelector, useDisk, keyComparator, groupByKey,
        edge.getEdgeID().nextId(), edge.getMessageSchema());
  }

  @Override
  public boolean send(int source, IMessage message, int flags) {
    TaskMessage<Tuple> taskMessage = (TaskMessage) message;
    return op.gather(source, taskMessage.getContent().getKey(),
        taskMessage.getContent().getValue(), flags);
  }

  private class GatherRecvrImpl implements BulkReceiver {
    @Override
    public void init(Config cfg, Set<Integer> expectedIds) {
    }

    @Override
    public boolean receive(int target, Iterator<Object> it) {
      TaskMessage msg = new TaskMessage<>(it, inEdge, target);
      BlockingQueue<IMessage> messages = outMessages.get(target);
      if (messages != null) {
        return messages.offer(msg);
      } else {
        throw new RuntimeException("Un-expected message for target: " + target);
      }
    }

    @Override
    public boolean sync(int target, byte[] message) {
      return syncs.get(target).sync(inEdge, message);
    }
  }

  @Override
  public BaseOperation getOp() {
    return this.op;
  }
}
