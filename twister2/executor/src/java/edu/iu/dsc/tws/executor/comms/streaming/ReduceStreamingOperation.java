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
import edu.iu.dsc.tws.comms.api.ReduceFunction;
import edu.iu.dsc.tws.comms.api.SingularReceiver;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.op.Communicator;
import edu.iu.dsc.tws.comms.op.stream.SReduce;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.executor.core.AbstractParallelOperation;
import edu.iu.dsc.tws.executor.core.EdgeGenerator;
import edu.iu.dsc.tws.executor.util.Utils;
import edu.iu.dsc.tws.task.api.IFunction;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.TaskMessage;

/**
 * Connecting to the reduce operation
 */
public class ReduceStreamingOperation extends AbstractParallelOperation {
  private static final Logger LOG = Logger.getLogger(ReduceStreamingOperation.class.getName());

  protected SReduce op;

  protected IFunction function;

  public ReduceStreamingOperation(Config config, Communicator network,
                                  TaskPlan tPlan, IFunction fnc,
                                  Set<Integer> sources, int dest, EdgeGenerator e,
                                  DataType dataType, String edgeName) {
    super(config, network, tPlan);
    this.function = fnc;
    this.edgeGenerator = e;

    if (sources.size() == 0) {
      throw new IllegalArgumentException("Sources should have more than 0 elements");
    }

    if (fnc == null) {
      throw new IllegalArgumentException("Operation expects a function");
    }

    op = new SReduce(channel, taskPlan, sources, dest, Utils.dataTypeToMessageType(dataType),
        new ReduceFunctionImpl(function), new FinalSingularReceiver());
  }

  @Override
  public boolean send(int source, IMessage message, int flags) {
    return op.reduce(source, message.getContent(), flags);
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

  private class FinalSingularReceiver implements SingularReceiver {
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
