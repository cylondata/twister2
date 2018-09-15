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

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.BulkReceiver;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.op.Communicator;
import edu.iu.dsc.tws.comms.op.batch.BKeyedGather;
import edu.iu.dsc.tws.comms.op.selectors.HashingSelector;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.executor.core.AbstractParallelOperation;
import edu.iu.dsc.tws.executor.core.EdgeGenerator;
import edu.iu.dsc.tws.executor.util.Utils;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.TaskMessage;

public class KeyedGatherBatchOperation extends AbstractParallelOperation {
  protected BKeyedGather op;

  public KeyedGatherBatchOperation(Config config, Communicator network, TaskPlan tPlan,
                                   Set<Integer> sources, Set<Integer> dests, EdgeGenerator e,
                                   DataType dataType, DataType keyType,
                                   String edgeName) {
    super(config, network, tPlan);
    this.edgeGenerator = e;
    op = new BKeyedGather(channel, taskPlan, sources, dests,
        Utils.dataTypeToMessageType(keyType),
        Utils.dataTypeToMessageType(dataType), new GatherRecvrImpl(), new HashingSelector());

    communicationEdge = e.generate(edgeName);
  }

  @Override
  public boolean send(int source, IMessage message, int flags) {
    TaskMessage taskMessage = (TaskMessage) message;
    return op.gather(source, taskMessage.getKey(), taskMessage.getContent(), flags);
  }

  @Override
  public boolean progress() {
    return op.progress() || op.hasPending();
  }

  private class GatherRecvrImpl implements BulkReceiver {
    @Override
    public void init(Config cfg, Set<Integer> expectedIds) {
    }

    @Override
    public boolean receive(int target, Iterator<Object> it) {
      TaskMessage msg = new TaskMessage(it,
          edgeGenerator.getStringMapping(communicationEdge), target);
      BlockingQueue<IMessage> messages = outMessages.get(target);
      if (messages != null) {
        return messages.offer(msg);
      } else {
        throw new RuntimeException("Un-expected message for target: " + target);
      }
    }
  }

  @Override
  public void finish(int source) {
    op.finish(source);
  }
}
