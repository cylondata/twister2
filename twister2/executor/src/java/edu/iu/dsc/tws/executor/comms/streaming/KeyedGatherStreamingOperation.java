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

import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.BulkReceiver;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.op.Communicator;
import edu.iu.dsc.tws.comms.op.selectors.HashingSelector;
import edu.iu.dsc.tws.comms.op.stream.SKeyedGather;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.executor.core.AbstractParallelOperation;
import edu.iu.dsc.tws.executor.core.EdgeGenerator;
import edu.iu.dsc.tws.executor.util.Utils;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.TaskMessage;

public class KeyedGatherStreamingOperation extends AbstractParallelOperation {

  private static final Logger LOG = Logger.getLogger(KeyedGatherStreamingOperation.class.getName());

  private SKeyedGather op;

  public KeyedGatherStreamingOperation(Config config, Communicator network, TaskPlan tPlan,
                                       Set<Integer> sources, Set<Integer> dests, EdgeGenerator e,
                                       DataType dataType, DataType keyType,
                                       String edgeName) {
    super(config, network, tPlan);
    this.edgeGenerator = e;
    op = new SKeyedGather(channel, taskPlan, sources, dests,
        Utils.dataTypeToMessageType(keyType),
        Utils.dataTypeToMessageType(dataType), new GatherRecvrImpl(), new HashingSelector());
    communicationEdge = e.generate(edgeName);

  }

  @Override
  public boolean send(int source, IMessage message, int flags) {
    TaskMessage taskMessage = (TaskMessage) message;
    LOG.info("Message : " + taskMessage.getContent());
    return op.reduce(source, taskMessage.getKey(), taskMessage.getContent(), flags);
  }

  @Override
  public boolean progress() {
    return op.progress();
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
        LOG.info("Message @receiver : " + msg.getContent());
        return messages.offer(msg);
      } else {
        throw new RuntimeException("Un-expected message for target: " + target);
      }
    }
  }
}
