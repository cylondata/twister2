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
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.dfw.DataFlowBroadcast;
import edu.iu.dsc.tws.comms.op.Communicator;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.executor.comms.AbstractParallelOperation;
import edu.iu.dsc.tws.executor.core.EdgeGenerator;
import edu.iu.dsc.tws.executor.util.Utils;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.TaskMessage;

public class BroadcastBatchOperation extends AbstractParallelOperation {
  private static final Logger LOG = Logger.getLogger(BroadcastBatchOperation.class.getName());
  private DataFlowBroadcast op;

  public BroadcastBatchOperation(Config config, Communicator network, TaskPlan tPlan) {
    super(config, network, tPlan);
  }

  public void prepare(int srcs, Set<Integer> dests, EdgeGenerator e,
                      DataType dataType, String edgeName) {
    if (dests.size() == 0) {
      throw new IllegalArgumentException("Targets should have more than 0 elements");
    }

    this.edgeGenerator = e;
    op = new DataFlowBroadcast(channel.getChannel(), srcs, dests, new BcastReceiver());
    communicationEdge = e.generate(edgeName);
    op.init(config, Utils.dataTypeToMessageType(dataType), taskPlan, communicationEdge);
  }

  @Override
  public boolean send(int source, IMessage message, int flags) {
    return op.send(source, message.getContent(), flags);
  }

  @Override
  public boolean progress() {
    return op.progress() || hasPending();
  }

  public boolean hasPending() {
    return !op.isComplete();
  }

  public class BcastReceiver implements MessageReceiver {
    @Override
    public void init(Config cfg, DataFlowOperation operation,
                     Map<Integer, List<Integer>> expectedIds) {
    }

    @Override
    public boolean onMessage(int source, int path, int target, int flags, Object object) {
      TaskMessage msg = new TaskMessage(object,
          edgeGenerator.getStringMapping(communicationEdge), target);
      return outMessages.get(target).offer(msg);
    }

    @Override
    public boolean progress() {
      return false;
    }
  }

  @Override
  public void close() {
    op.close();
  }
}
