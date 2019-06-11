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

import java.util.Comparator;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.BlockingQueue;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.BulkReceiver;
import edu.iu.dsc.tws.comms.api.Communicator;
import edu.iu.dsc.tws.comms.api.DestinationSelector;
import edu.iu.dsc.tws.comms.api.TaskPlan;
import edu.iu.dsc.tws.comms.api.batch.BJoin;
import edu.iu.dsc.tws.comms.api.selectors.HashingSelector;
import edu.iu.dsc.tws.comms.dfw.io.Tuple;
import edu.iu.dsc.tws.executor.api.IBinaryParallelOperation;
import edu.iu.dsc.tws.executor.comms.AbstractParallelOperation;
import edu.iu.dsc.tws.executor.comms.DefaultDestinationSelector;
import edu.iu.dsc.tws.executor.util.Utils;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.TaskMessage;
import edu.iu.dsc.tws.task.graph.Edge;

public class JoinBatchOperation extends AbstractParallelOperation
    implements IBinaryParallelOperation {
  protected BJoin op;

  public JoinBatchOperation(Config config, Communicator network, TaskPlan tPlan,
                            Set<Integer> sources, Set<Integer> dests,
                            Edge leftEdge, Edge rightEdge) {
    super(config, network, tPlan, leftEdge.getGroup());

    DestinationSelector destSelector;
    if (leftEdge.getPartitioner() != null) {
      destSelector = new DefaultDestinationSelector(leftEdge.getPartitioner());
    } else {
      destSelector = new HashingSelector();
    }

    boolean useDisk = false;
    Comparator keyComparator = null;
    try {
      useDisk = (Boolean) leftEdge.getProperty("use-disk");
      keyComparator = (Comparator) leftEdge.getProperty("key-comparator");
    } catch (Exception ex) {
      //ignore
    }

    Communicator newComm = channel.newWithConfig(leftEdge.getProperties());
    op = new BJoin(newComm, taskPlan, sources, dests,
        Utils.dataTypeToMessageType(leftEdge.getKeyType()),
        Utils.dataTypeToMessageType(leftEdge.getDataType()),
        Utils.dataTypeToMessageType(rightEdge.getDataType()),
        new GatherRecvrImpl(), destSelector, useDisk, keyComparator);
  }

  @Override
  public boolean send(int source, IMessage message, int flags) {
    TaskMessage<Tuple> taskMessage = (TaskMessage) message;
    return op.join(source, taskMessage.getContent().getKey(),
        taskMessage.getContent().getValue(), flags, 0);
  }

  @Override
  public boolean progress() {
    return op.progress() || op.hasPending();
  }

  @Override
  public boolean sendRight(int source, IMessage message, int flags) {
    TaskMessage<Tuple> taskMessage = (TaskMessage) message;
    return op.join(source, taskMessage.getContent().getKey(),
        taskMessage.getContent().getValue(), flags, 1);
  }

  private class GatherRecvrImpl implements BulkReceiver {
    @Override
    public void init(Config cfg, Set<Integer> expectedIds) {
    }

    @Override
    public boolean receive(int target, Iterator<Object> it) {
      TaskMessage msg = new TaskMessage<>(it, inEdge, target);
      BlockingQueue<IMessage> messages = outMessages.get(target);
      if (messages != null) {
        return messages.offer(msg);
      } else {
        throw new RuntimeException("Un-expected message for target: " + target);
      }
    }

    @Override
    public boolean sync(int target, byte[] message) {
      return syncs.get(target).sync(inEdge, message);
    }
  }

  @Override
  public void finish(int source) {
    op.finish(source);
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
    return !op.hasPending();
  }
}
