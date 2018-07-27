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
package edu.iu.dsc.tws.executor.comm.operations.batch;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.api.ReduceFunction;
import edu.iu.dsc.tws.comms.api.ReduceReceiver;
import edu.iu.dsc.tws.comms.api.TWSChannel;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.dfw.DataFlowReduce;
import edu.iu.dsc.tws.comms.dfw.io.reduce.ReduceBatchFinalReceiver;
import edu.iu.dsc.tws.comms.dfw.io.reduce.ReduceBatchPartialReceiver;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.executor.api.AbstractParallelOperation;
import edu.iu.dsc.tws.executor.api.EdgeGenerator;
import edu.iu.dsc.tws.executor.util.Utils;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.TaskMessage;

public class ReduceBatchOperation extends AbstractParallelOperation {

  private static final Logger LOG = Logger.getLogger(ReduceBatchOperation.class.getName());

  protected DataFlowReduce op;

  public ReduceBatchOperation(Config config, TWSChannel network, TaskPlan tPlan) {
    super(config, network, tPlan);
  }

  public void prepare(Set<Integer> sources, int dest, EdgeGenerator e,
                      DataType dataType, String edgeName) {
    this.edge = e;
    op = new DataFlowReduce(channel, sources, dest,
        new ReduceBatchFinalReceiver(new IdentityFunction(), new FinalReduceReceiver()),
        new ReduceBatchPartialReceiver(dest, new IdentityFunction()));
    communicationEdge = e.generate(edgeName);
    LOG.info("===Communication Edge : " + communicationEdge);
    op.init(config, Utils.dataTypeToMessageType(dataType), taskPlan, communicationEdge);
  }

  @Override
  public boolean send(int source, IMessage message, int flags) {
    //LOG.log(Level.INFO, String.format("Message %s", message.getContent()));
    return op.send(source, message.getContent(), flags);
  }

  @Override
  public void send(int source, IMessage message, int dest, int flags) {
    op.send(source, message, flags, dest);
  }

  @Override
  public void progress() {
    op.progress();
  }


  public static class IdentityFunction implements ReduceFunction {
    @Override
    public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    }

    @Override
    public Object reduce(Object t1, Object t2) {
      return t1;
    }
  }


  public class FinalReduceReceiver implements ReduceReceiver, MessageReceiver {
    private int count = 0;

    @Override
    public void init(Config cfg, DataFlowOperation operation,
                     Map<Integer, List<Integer>> expectedIds) {

    }

    @Override
    public boolean onMessage(int source, int destination, int target, int flags, Object object) {
      return false;
    }

    @Override
    public void onFinish(int target) {
      LOG.info("OnFinish : " + target);
      //op.finish(target);
    }

    @Override
    public void progress() {
      //LOG.log(Level.INFO, String.format("Comes to Progress "));
    }

    @Override
    public boolean receive(int target, Object object) {
      TaskMessage msg = new TaskMessage(object,
          edge.getStringMapping(communicationEdge), target);
      outMessages.get(target).offer(msg);
      return true;
    }
  }

}
