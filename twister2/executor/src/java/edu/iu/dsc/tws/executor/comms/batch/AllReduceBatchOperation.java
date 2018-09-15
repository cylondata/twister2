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
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.ReduceFunction;
import edu.iu.dsc.tws.comms.api.SingularReceiver;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.op.Communicator;
import edu.iu.dsc.tws.comms.op.batch.BAllReduce;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.executor.core.AbstractParallelOperation;
import edu.iu.dsc.tws.executor.core.EdgeGenerator;
import edu.iu.dsc.tws.executor.util.Utils;
import edu.iu.dsc.tws.task.api.IFunction;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.TaskMessage;

public class AllReduceBatchOperation extends AbstractParallelOperation {
  private static final Logger LOG = Logger.getLogger(AllReduceBatchOperation.class.getName());

  protected BAllReduce op;

  public AllReduceBatchOperation(Config config, Communicator network, TaskPlan tPlan,
                                 Set<Integer> sources, Set<Integer>  dest, EdgeGenerator e,
                                 DataType dataType, String edgeName, IFunction fnc) {
    super(config, network, tPlan);
    this.edgeGenerator = e;
    op = new BAllReduce(channel, taskPlan, sources, dest,
        new ReduceFnImpl(fnc),
        new FinalSingularReceiver(), Utils.dataTypeToMessageType(dataType));
    communicationEdge = e.generate(edgeName);
  }

  @Override
  public boolean send(int source, IMessage message, int flags) {
    //LOG.log(Level.INFO, String.format("Message %s", message.getContent()));
    return op.reduce(source, message.getContent(), flags);
  }

  @Override
  public void finish(int source) {
    op.finish(source);
  }

  @Override
  public boolean progress() {
    return op.progress() || op.hasPending();
  }

  public static class ReduceFnImpl implements ReduceFunction {
    private IFunction fn;

    public ReduceFnImpl(IFunction fn) {
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
    private int count = 0;

    @Override
    public void init(Config cfg, Set<Integer> expectedIds) {
    }

    @Override
    public boolean receive(int target, Object object) {
      TaskMessage msg = new TaskMessage(object,
          edgeGenerator.getStringMapping(communicationEdge), target);
      return outMessages.get(target).offer(msg);
    }
  }
}
