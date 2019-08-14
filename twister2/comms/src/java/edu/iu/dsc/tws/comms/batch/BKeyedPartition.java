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
package edu.iu.dsc.tws.comms.batch;

import java.util.Set;

import edu.iu.dsc.tws.api.comms.BulkReceiver;
import edu.iu.dsc.tws.api.comms.Communicator;
import edu.iu.dsc.tws.api.comms.DestinationSelector;
import edu.iu.dsc.tws.api.comms.LogicalPlan;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageType;
import edu.iu.dsc.tws.api.comms.packing.MessageSchema;
import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.comms.dfw.BaseOperation;
import edu.iu.dsc.tws.comms.dfw.MToNSimple;
import edu.iu.dsc.tws.comms.dfw.io.partition.PartitionBatchFinalReceiver;
import edu.iu.dsc.tws.comms.dfw.io.partition.PartitionPartialReceiver;

public class BKeyedPartition extends BaseOperation {
  private DestinationSelector destinationSelector;

  public BKeyedPartition(Communicator comm, LogicalPlan plan,
                         Set<Integer> sources, Set<Integer> destinations,
                         MessageType keyType, MessageType dataType,
                         BulkReceiver rcvr, DestinationSelector destSelector,
                         int edgeId, MessageSchema messageSchema) {
    super(comm.getChannel());
    this.destinationSelector = destSelector;
    MToNSimple partition = new MToNSimple(comm.getChannel(), sources, destinations,
        new PartitionBatchFinalReceiver(rcvr),
        new PartitionPartialReceiver(), dataType, keyType, messageSchema);
    partition.init(comm.getConfig(), dataType, plan, edgeId);
    this.destinationSelector.prepare(comm, partition.getSources(), partition.getTargets());
    this.op = partition;
  }

  public BKeyedPartition(Communicator comm, LogicalPlan plan,
                         Set<Integer> sources, Set<Integer> destinations,
                         MessageType keyType, MessageType dataType,
                         BulkReceiver rcvr, DestinationSelector destSelector) {
    this(comm, plan, sources, destinations, keyType, dataType, rcvr, destSelector,
        comm.nextEdge(), MessageSchema.noSchema());
  }

  public BKeyedPartition(Communicator comm, LogicalPlan plan,
                         Set<Integer> sources, Set<Integer> destinations,
                         MessageType keyType, MessageType dataType,
                         BulkReceiver rcvr, DestinationSelector destSelector,
                         MessageSchema messageSchema) {
    this(comm, plan, sources, destinations, keyType, dataType, rcvr, destSelector,
        comm.nextEdge(), messageSchema);
  }

  public boolean partition(int source, Object key, Object message, int flags) {
    int destinations = destinationSelector.next(source, key, message);

    return op.send(source, Tuple.of(key, message, op.getKeyType(),
        op.getDataType()), flags, destinations);
  }
}
