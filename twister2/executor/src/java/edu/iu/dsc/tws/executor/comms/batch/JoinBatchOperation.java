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
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.comms.BulkReceiver;
import edu.iu.dsc.tws.api.comms.Communicator;
import edu.iu.dsc.tws.api.comms.DestinationSelector;
import edu.iu.dsc.tws.api.comms.LogicalPlan;
import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.task.IMessage;
import edu.iu.dsc.tws.api.task.TaskMessage;
import edu.iu.dsc.tws.api.task.graph.Edge;
import edu.iu.dsc.tws.comms.batch.BJoin;
import edu.iu.dsc.tws.comms.selectors.HashingSelector;
import edu.iu.dsc.tws.executor.comms.AbstractParallelOperation;
import edu.iu.dsc.tws.executor.comms.DefaultDestinationSelector;

public class JoinBatchOperation extends AbstractParallelOperation {
  private static final Logger LOG = Logger.getLogger(JoinBatchOperation.class.getName());

  protected BJoin op;

  private Edge leftEdge;

  private Edge rightEdge;

  private Set<Integer> finishedSources = new HashSet<>();

  public JoinBatchOperation(Config config, Communicator network, LogicalPlan tPlan,
                            Set<Integer> sources, Set<Integer> dests,
                            Edge leftEdge, Edge rightEdge) {
    super(config, network, tPlan, leftEdge.getTargetEdge());
    this.leftEdge = leftEdge;
    this.rightEdge = rightEdge;

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
    op = new BJoin(newComm, logicalPlan, sources, dests,
        leftEdge.getKeyType(),
        leftEdge.getDataType(),
        rightEdge.getDataType(),
        new JoinRecvrImpl(), destSelector, useDisk,
        keyComparator, leftEdge.getEdgeID().nextId(), rightEdge.getEdgeID().nextId());
  }

  @Override
  public boolean send(int source, IMessage message, int flags) {
    TaskMessage<Tuple> taskMessage = (TaskMessage) message;
    if (message.edge().equals(leftEdge.getName())) {
      return op.join(source, taskMessage.getContent().getKey(),
          taskMessage.getContent().getValue(), flags, 0);
    } else {
      return op.join(source, taskMessage.getContent().getKey(),
          taskMessage.getContent().getValue(), flags, 1);
    }
  }

  @Override
  public boolean progress() {
    return op.progress() || op.hasPending();
  }

  private class JoinRecvrImpl implements BulkReceiver {
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
    if (!finishedSources.contains(source)) {
      op.finish(source);
      finishedSources.add(source);
    }
  }

  @Override
  public void close() {
    op.close();
    finishedSources.clear();
  }

  @Override
  public void reset() {
    op.reset();
    finishedSources.clear();
  }

  @Override
  public boolean isComplete() {
    return !op.hasPending();
  }
}
