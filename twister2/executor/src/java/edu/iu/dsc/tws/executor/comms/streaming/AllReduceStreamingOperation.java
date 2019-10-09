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

import edu.iu.dsc.tws.api.comms.Communicator;
import edu.iu.dsc.tws.api.comms.DataFlowOperation;
import edu.iu.dsc.tws.api.comms.LogicalPlan;
import edu.iu.dsc.tws.api.comms.ReduceFunction;
import edu.iu.dsc.tws.api.comms.SingularReceiver;
import edu.iu.dsc.tws.api.compute.IFunction;
import edu.iu.dsc.tws.api.compute.IMessage;
import edu.iu.dsc.tws.api.compute.TaskMessage;
import edu.iu.dsc.tws.api.compute.graph.Edge;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.comms.dfw.BaseOperation;
import edu.iu.dsc.tws.comms.stream.SAllReduce;
import edu.iu.dsc.tws.executor.comms.AbstractParallelOperation;

public class AllReduceStreamingOperation extends AbstractParallelOperation {

  protected SAllReduce op;

  public AllReduceStreamingOperation(Config config, Communicator network,
                                     LogicalPlan tPlan, IFunction function,
                                     Set<Integer> sources, Set<Integer> dest, Edge edge) {
    super(config, network, tPlan, edge.getName());
    if (sources.size() == 0) {
      throw new IllegalArgumentException("Sources should have more than 0 elements");
    }

    if (dest.size() == 0) {
      throw new IllegalArgumentException("Targets should have more than 0 elements");
    }

    if (function == null) {
      throw new IllegalArgumentException("Operation expects a function");
    }

    Communicator newComm = channel.newWithConfig(edge.getProperties());
    op = new SAllReduce(newComm, logicalPlan, sources, dest, edge.getDataType(),
        new ReduceFnImpl(function), new FinalSingularReceive(),
        edge.getEdgeID().nextId(), edge.getEdgeID().nextId(), edge.getMessageSchema());
  }

  @Override
  public boolean send(int source, IMessage message, int flags) {
    return op.reduce(source, message.getContent(), flags);
  }

  private static class ReduceFnImpl implements ReduceFunction {
    private IFunction fn;

    ReduceFnImpl(IFunction function) {
      this.fn = function;
    }

    @Override
    public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    }

    @Override
    public Object reduce(Object t1, Object t2) {
      return fn.onMessage(t1, t2);
    }
  }

  private class FinalSingularReceive implements SingularReceiver {
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
  protected BaseOperation getOp() {
    return this.op;
  }
}
