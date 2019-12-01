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
package edu.iu.dsc.tws.comms.stream;

import java.util.Set;

import edu.iu.dsc.tws.api.comms.BaseOperation;
import edu.iu.dsc.tws.api.comms.CommunicationContext;
import edu.iu.dsc.tws.api.comms.Communicator;
import edu.iu.dsc.tws.api.comms.DestinationSelector;
import edu.iu.dsc.tws.api.comms.LogicalPlan;
import edu.iu.dsc.tws.api.comms.SingularReceiver;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageType;
import edu.iu.dsc.tws.api.comms.packing.MessageSchema;
import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.comms.dfw.MToNSimple;
import edu.iu.dsc.tws.comms.dfw.io.TargetPartialReceiver;
import edu.iu.dsc.tws.comms.dfw.io.partition.PartitionStreamingFinalReceiver;
import edu.iu.dsc.tws.comms.utils.LogicalPlanBuilder;

/**
 * Streaming Keyed Partition Operation
 */
public class SKeyedPartition extends BaseOperation {
  /**
   * Destination selector
   */
  private DestinationSelector destinationSelector;

  /**
   * Construct a Streaming Key based partition operation
   *
   * @param comm the communicator
   * @param plan task plan
   * @param sources source tasks
   * @param targets target tasks
   * @param rcvr receiver
   * @param dataType data type
   */
  public SKeyedPartition(Communicator comm, LogicalPlan plan,
                         Set<Integer> sources, Set<Integer> targets,
                         MessageType keyType, MessageType dataType, SingularReceiver rcvr,
                         DestinationSelector destSelector, int edgeId,
                         MessageSchema messageSchema) {
    super(comm, true, CommunicationContext.KEYED_PARTITION);
    this.destinationSelector = destSelector;
    MToNSimple partition = new MToNSimple(comm.getChannel(), sources, targets,
        new PartitionStreamingFinalReceiver(rcvr), new TargetPartialReceiver(),
        dataType, keyType, messageSchema);

    partition.init(comm.getConfig(), dataType, plan, edgeId);
    this.destinationSelector.prepare(comm, partition.getSources(), partition.getTargets(),
        keyType, dataType);
    op = partition;
  }

  public SKeyedPartition(Communicator comm, LogicalPlan plan,
                         Set<Integer> sources, Set<Integer> targets,
                         MessageType keyType, MessageType dataType, SingularReceiver rcvr,
                         DestinationSelector destSelector) {
    this(comm, plan, sources, targets, keyType, dataType, rcvr, destSelector,
        comm.nextEdge(), MessageSchema.noSchema());
  }

  public SKeyedPartition(Communicator comm, LogicalPlanBuilder logicalPlanBuilder,
                         MessageType keyType, MessageType dataType, SingularReceiver rcvr,
                         DestinationSelector destSelector) {
    this(comm, logicalPlanBuilder.build(),
        logicalPlanBuilder.getSources(),
        logicalPlanBuilder.getTargets(), keyType, dataType, rcvr, destSelector,
        comm.nextEdge(), MessageSchema.noSchema());
  }

  public SKeyedPartition(Communicator comm, LogicalPlan plan,
                         Set<Integer> sources, Set<Integer> targets,
                         MessageType keyType, MessageType dataType, SingularReceiver rcvr,
                         DestinationSelector destSelector, MessageSchema messageSchema) {
    this(comm, plan, sources, targets, keyType, dataType, rcvr, destSelector,
        comm.nextEdge(), messageSchema);
  }

  /**
   * Send a message to be partitioned based on the key
   *
   * @param src source
   * @param key key
   * @param message message
   * @param flags message flag
   * @return true if the message is accepted
   */
  public boolean partition(int src, Object key, Object message, int flags) {
    int dest = destinationSelector.next(src, key, message);

    boolean send = op.send(src, new Tuple<>(key, message), flags, dest);
    if (send) {
      destinationSelector.commit(src, dest);
    }
    return send;
  }

  /**
   * Send a message to be partitioned based on the key
   *
   * @param src source
   * @param data tuple
   * @param flags message flag
   * @return true if the message is accepted
   */
  public boolean partition(int src, Tuple data, int flags) {
    int dest = destinationSelector.next(src, data.getKey(), data.getValue());
    boolean send = op.send(src, data, flags, dest);
    if (send) {
      destinationSelector.commit(src, dest);
    }
    return send;
  }
}
