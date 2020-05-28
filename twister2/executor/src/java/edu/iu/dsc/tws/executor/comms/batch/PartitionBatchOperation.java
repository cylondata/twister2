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

import edu.iu.dsc.tws.api.comms.BaseOperation;
import edu.iu.dsc.tws.api.comms.BulkReceiver;
import edu.iu.dsc.tws.api.comms.Communicator;
import edu.iu.dsc.tws.api.comms.DestinationSelector;
import edu.iu.dsc.tws.api.comms.LogicalPlan;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageType;
import edu.iu.dsc.tws.api.compute.IMessage;
import edu.iu.dsc.tws.api.compute.TaskMessage;
import edu.iu.dsc.tws.api.compute.graph.Edge;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.comms.batch.BPartition;
import edu.iu.dsc.tws.comms.selectors.HashingSelector;
import edu.iu.dsc.tws.executor.comms.AbstractParallelOperation;
import edu.iu.dsc.tws.executor.comms.DefaultDestinationSelector;

public class PartitionBatchOperation extends AbstractParallelOperation {

  protected BPartition op;

  public PartitionBatchOperation(Config config, Communicator network, LogicalPlan tPlan,
                                 Set<Integer> sources, Set<Integer> targets, Edge edge) {
    super(config, network, tPlan, edge.getName());
    MessageType dataType = edge.getDataType();
    String edgeName = edge.getName();
    boolean shuffle = false;

    DestinationSelector destSelector;
    if (edge.getPartitioner() != null) {
      destSelector = new DefaultDestinationSelector(edge.getPartitioner());
    } else {
      destSelector = new HashingSelector();
    }

    Object shuffleProp = edge.getProperty("shuffle");
    if (shuffleProp instanceof Boolean && (Boolean) shuffleProp) {
      shuffle = true;
    }
    Communicator newComm = channel.newWithConfig(edge.getProperties());

    //LOG.info("ParitionOperation Prepare 1");
    op = new BPartition(newComm, logicalPlan, sources, targets,
        dataType, new PartitionReceiver(),
        destSelector, shuffle, edge.getEdgeID().nextId(), edge.getMessageSchema());
  }

  public boolean send(int source, IMessage message, int flags) {
    return op.partition(source, message.getContent(), 0);
  }

  public class PartitionReceiver implements BulkReceiver {
    @Override
    public void init(Config cfg, Set<Integer> expectedIds) {
    }

    @Override
    public boolean receive(int target, Iterator<Object> it) {
      TaskMessage msg = new TaskMessage<>(it, inEdge, target);
      return outMessages.get(target).offer(msg);
    }

    @Override
    public boolean sync(int target, byte[] message) {
      return syncs.get(target).sync(inEdge, message);
    }
  }

  @Override
  public BaseOperation getOp() {
    return this.op;
  }
}
