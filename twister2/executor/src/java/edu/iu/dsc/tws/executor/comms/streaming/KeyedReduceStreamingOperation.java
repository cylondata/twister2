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
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.api.ReduceFunction;
import edu.iu.dsc.tws.comms.api.SingularReceiver;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.op.Communicator;
import edu.iu.dsc.tws.comms.op.selectors.HashingSelector;
import edu.iu.dsc.tws.comms.op.stream.SKeyedReduce;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.executor.core.AbstractParallelOperation;
import edu.iu.dsc.tws.executor.core.EdgeGenerator;
import edu.iu.dsc.tws.executor.util.Utils;
import edu.iu.dsc.tws.task.api.IFunction;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.TaskMessage;

public class KeyedReduceStreamingOperation extends AbstractParallelOperation {
  private static final Logger LOG = Logger.getLogger(KeyedReduceStreamingOperation.class.getName());

  private SKeyedReduce op;

  public KeyedReduceStreamingOperation(Config config, Communicator network, TaskPlan tPlan,
                                       Set<Integer> sources, Set<Integer> dests, EdgeGenerator e,
                                       DataType dType, DataType kType,
                                       String edgeName, IFunction fn) {
    super(config, network, tPlan);

    if (sources.size() == 0) {
      throw new RuntimeException("Sources should have more than 0 elements");
    }

    if (dests.size() == 0) {
      throw new IllegalArgumentException("Targets should have more than 0 elements");
    }

    MessageType dataType = Utils.dataTypeToMessageType(dType);
    MessageType keyType = Utils.dataTypeToMessageType(kType);

    this.edgeGenerator = e;
    op = new SKeyedReduce(channel, taskPlan, sources, dests, keyType, dataType,
        new ReduceFunctionImpl(fn), new SingularRecvrImpl(), new HashingSelector());
  }

  @Override
  public boolean send(int source, IMessage message, int flags) {
    TaskMessage taskMessage = (TaskMessage) message;
    return op.reduce(source, taskMessage.getKey(), taskMessage.getContent(), flags);
  }

  @Override
  public boolean progress() {
    return op.progress();
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
      TaskMessage msg = new TaskMessage(object,
          edgeGenerator.getStringMapping(communicationEdge), target);
      BlockingQueue<IMessage> messages = outMessages.get(target);
      if (messages != null) {
        if (messages.offer(msg)) {
          return true;
        }
      }
      return true;
    }
  }
}
