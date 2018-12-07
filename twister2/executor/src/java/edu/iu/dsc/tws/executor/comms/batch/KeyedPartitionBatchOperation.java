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
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.BulkReceiver;
import edu.iu.dsc.tws.comms.api.DestinationSelector;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.op.Communicator;
import edu.iu.dsc.tws.comms.op.batch.BKeyedPartition;
import edu.iu.dsc.tws.comms.op.selectors.HashingSelector;
import edu.iu.dsc.tws.data.api.DataType;
import edu.iu.dsc.tws.executor.comms.AbstractParallelOperation;
import edu.iu.dsc.tws.executor.comms.DefaultDestinationSelector;
import edu.iu.dsc.tws.executor.core.EdgeGenerator;
import edu.iu.dsc.tws.executor.util.Utils;
import edu.iu.dsc.tws.task.api.IMessage;
import edu.iu.dsc.tws.task.api.TaskKeySelector;
import edu.iu.dsc.tws.task.api.TaskMessage;
import edu.iu.dsc.tws.task.api.TaskPartitioner;

public class KeyedPartitionBatchOperation extends AbstractParallelOperation {
  private static final Logger LOG = Logger.getLogger(KeyedPartitionBatchOperation.class.getName());

  private BKeyedPartition op;

  private TaskKeySelector selector;

  public KeyedPartitionBatchOperation(Config config, Communicator network, TaskPlan tPlan,
                                      Set<Integer> srcs, Set<Integer> dests, EdgeGenerator e,
                                      DataType dataType, DataType keyType, String edgeName,
                                      boolean shuffle,
                                      TaskPartitioner partitioner, TaskKeySelector selec) {
    super(config, network, tPlan);
    this.edgeGenerator = e;
    this.selector = selec;

    DestinationSelector destSelector = null;
    if (selec != null) {
      destSelector = new DefaultDestinationSelector(partitioner);
    } else {
      destSelector = new HashingSelector();
    }

    op = new BKeyedPartition(channel, taskPlan, srcs, dests, Utils.dataTypeToMessageType(dataType),
        Utils.dataTypeToMessageType(keyType), new PartitionReceiver(), destSelector);
    communicationEdge = e.generate(edgeName);
  }

  @Override
  public boolean send(int source, IMessage message, int flags) {
    TaskMessage taskMessage = (TaskMessage) message;
    Object key = extractKey(taskMessage, selector);
    return op.partition(source, key, taskMessage.getContent(), flags);
  }

  @Override
  public boolean progress() {
    return op.progress() || op.hasPending();
  }

  @Override
  public void finish(int source) {
    op.finish(source);
  }

  public class PartitionReceiver implements BulkReceiver {
    @Override
    public void init(Config cfg, Set<Integer> expectedIds) {
    }

    @Override
    public boolean receive(int target, Iterator<Object> it) {
      TaskMessage msg = new TaskMessage(it,
          edgeGenerator.getStringMapping(communicationEdge), target);
      return outMessages.get(target).offer(msg);
    }
  }
}
