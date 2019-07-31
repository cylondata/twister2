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

import java.util.List;
import java.util.Map;
import java.util.Set;

import edu.iu.dsc.tws.api.comms.Communicator;
import edu.iu.dsc.tws.api.comms.DataFlowOperation;
import edu.iu.dsc.tws.api.comms.LogicalPlan;
import edu.iu.dsc.tws.api.comms.ReduceFunction;
import edu.iu.dsc.tws.api.comms.SingularReceiver;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.task.IFunction;
import edu.iu.dsc.tws.api.task.IMessage;
import edu.iu.dsc.tws.api.task.TaskMessage;
import edu.iu.dsc.tws.api.task.graph.Edge;
import edu.iu.dsc.tws.comms.batch.BReduce;
import edu.iu.dsc.tws.executor.comms.AbstractParallelOperation;

public class ReduceBatchOperation extends AbstractParallelOperation {
  protected BReduce op;

  public ReduceBatchOperation(Config config, Communicator network, LogicalPlan tPlan,
                              Set<Integer> sources, Set<Integer> dests,
                              Edge edge) {
    super(config, network, tPlan, edge.getName());

    if (dests.size() > 1) {
      throw new RuntimeException("Reduce can only have one target: " + dests);
    }
    Communicator newComm = channel.newWithConfig(edge.getProperties());
    op = new BReduce(newComm, logicalPlan, sources, dests.iterator().next(),
        new ReduceFnImpl(edge.getFunction()),
        new FinalSingularReceiver(), edge.getDataType(), edge.getEdgeID().nextId());
  }

  @Override
  public boolean send(int source, IMessage message, int flags) {
    return op.reduce(source, message.getContent(), flags);
  }

  @Override
  public void finish(int source) {
    op.finish(source);
  }

  @Override
  public boolean progress() {
    return op.progress() || !op.isComplete();
  }

  public static class ReduceFnImpl implements ReduceFunction {
    private IFunction fn;

    ReduceFnImpl(IFunction fn) {
      this.fn = fn;
    }

    @Override
    public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    }

    @Override
    public Object reduce(Object t1, Object t2) {
      return fn.onMessage(t1, t2);
    }
  }

  public class FinalSingularReceiver implements SingularReceiver {
    @Override
    public void init(Config cfg, Set<Integer> expectedIds) {
    }

    @Override
    public boolean receive(int target, Object object) {
      TaskMessage msg = new TaskMessage<>(object, inEdge, target);
      return outMessages.get(target).offer(msg);
    }

    @Override
    public boolean sync(int target, byte[] message) {
      return syncs.get(target).sync(inEdge, message);
    }
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
    return op.isComplete();
  }
}
