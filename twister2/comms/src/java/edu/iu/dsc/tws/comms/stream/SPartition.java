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

import edu.iu.dsc.tws.api.comms.Communicator;
import edu.iu.dsc.tws.api.comms.DestinationSelector;
import edu.iu.dsc.tws.api.comms.LogicalPlan;
import edu.iu.dsc.tws.api.comms.SingularReceiver;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageType;
import edu.iu.dsc.tws.comms.dfw.BaseOperation;
import edu.iu.dsc.tws.comms.dfw.MToNSimple;
import edu.iu.dsc.tws.comms.dfw.io.partition.PartitionStreamingFinalReceiver;
import edu.iu.dsc.tws.comms.dfw.io.partition.PartitionStreamingPartialReceiver;

/**
 * Streaming Partition Operation
 */
public class SPartition extends BaseOperation {
  /**
   * Destination selector
   */
  private DestinationSelector destinationSelector;

  /**
   * Construct a Streaming partition operation
   *
   * @param comm the communicator
   * @param plan task plan
   * @param sources source tasks
   * @param targets target tasks
   * @param rcvr receiver
   * @param dataType data type
   */
  public SPartition(Communicator comm, LogicalPlan plan,
                    Set<Integer> sources, Set<Integer> targets, MessageType dataType,
                    SingularReceiver rcvr,
                    DestinationSelector destSelector, int edgeId) {
    super(comm.getChannel());
    this.destinationSelector = destSelector;
    MToNSimple partition = new MToNSimple(comm.getChannel(), sources, targets,
        new PartitionStreamingFinalReceiver(rcvr),
        new PartitionStreamingPartialReceiver(), dataType);
    partition.init(comm.getConfig(), dataType, plan, edgeId);
    this.destinationSelector.prepare(comm, partition.getSources(), partition.getTargets());
    op = partition;
  }

  public SPartition(Communicator comm, LogicalPlan plan,
                    Set<Integer> sources, Set<Integer> targets, MessageType dataType,
                    SingularReceiver rcvr,
                    DestinationSelector destSelector) {
    this(comm, plan, sources, targets, dataType, rcvr, destSelector, comm.nextEdge());
  }

  /**
   * Send a message to be partitioned
   *
   * @param src source
   * @param message message
   * @param flags message flag
   * @return true if the message is accepted
   */
  public boolean partition(int src, Object message, int flags) {
    final int dest = destinationSelector.next(src, message);

    boolean send = op.send(src, message, flags, dest);
    if (send) {
      destinationSelector.commit(src, dest);
    }
    return send;
  }
}
