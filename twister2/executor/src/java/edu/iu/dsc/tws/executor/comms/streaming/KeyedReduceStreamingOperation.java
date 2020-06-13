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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

import edu.iu.dsc.tws.api.comms.BaseOperation;
import edu.iu.dsc.tws.api.comms.Communicator;
import edu.iu.dsc.tws.api.comms.DataFlowOperation;
import edu.iu.dsc.tws.api.comms.DestinationSelector;
import edu.iu.dsc.tws.api.comms.LogicalPlan;
import edu.iu.dsc.tws.api.comms.ReduceFunction;
import edu.iu.dsc.tws.api.comms.SingularReceiver;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageType;
import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.compute.IFunction;
import edu.iu.dsc.tws.api.compute.IMessage;
import edu.iu.dsc.tws.api.compute.TaskMessage;
import edu.iu.dsc.tws.api.compute.graph.Edge;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.comms.selectors.HashingSelector;
import edu.iu.dsc.tws.comms.stream.SKeyedReduce;
import edu.iu.dsc.tws.executor.comms.AbstractParallelOperation;
import edu.iu.dsc.tws.executor.comms.DefaultDestinationSelector;

public class KeyedReduceStreamingOperation extends AbstractParallelOperation {

  private SKeyedReduce op;

  public KeyedReduceStreamingOperation(Config config, Communicator network, LogicalPlan tPlan,
                                       Set<Integer> sources, Set<Integer> dests, Edge edge,
                                       Map<Integer, Integer> srcGlobalToIndex,
                                       Map<Integer, Integer> tgtsGlobalToIndex) {
    super(config, network, tPlan, edge.getName());

    if (sources.size() == 0) {
      throw new RuntimeException("Sources should have more than 0 elements");
    }

    if (dests.size() == 0) {
      throw new IllegalArgumentException("Targets should have more than 0 elements");
    }

    DestinationSelector destSelector;
    if (edge.getPartitioner() != null) {
      destSelector = new DefaultDestinationSelector(edge.getPartitioner(),
          srcGlobalToIndex, tgtsGlobalToIndex);
    } else {
      destSelector = new HashingSelector();
    }

    MessageType dataType = edge.getDataType();
    MessageType keyType = edge.getKeyType();

    Communicator newComm = channel.newWithConfig(edge.getProperties());
    op = new SKeyedReduce(newComm, logicalPlan, sources, dests, keyType, dataType,
        new ReduceFunctionImpl(edge.getFunction()),
        new SingularRecvrImpl(), destSelector, edge.getEdgeID().nextId(),
        edge.getMessageSchema());
  }

  @Override
  public boolean send(int source, IMessage message, int flags) {
    TaskMessage<Tuple> taskMessage = (TaskMessage) message;
    return op.reduce(source,
        taskMessage.getContent().getKey(), taskMessage.getContent().getValue(), flags);
  }

  private class ReduceFunctionImpl implements ReduceFunction {
    private IFunction fn;

    ReduceFunctionImpl(IFunction fn) {
      this.fn = fn;
    }

    @Override
    public void init(Config cfg, DataFlowOperation ops,
                     Map<Integer, List<Integer>> expectedIds) {
    }

    @Override
    public Object reduce(Object t1, Object t2) {
      return fn.onMessage(t1, t2);
    }
  }

  private class SingularRecvrImpl implements SingularReceiver {
    @Override
    public void init(Config cfg, Set<Integer> expectedIds) {
    }

    @Override
    public boolean receive(int target, Object object) {
      TaskMessage msg = new TaskMessage<>(object, inEdge, target);
      BlockingQueue<IMessage> messages = outMessages.get(target);
      if (messages != null) {
        if (messages.offer(msg)) {
          return true;
        }
      }
      return true;
    }
  }

  @Override
  public BaseOperation getOp() {
    return this.op;
  }
}
