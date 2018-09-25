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
package edu.iu.dsc.tws.comms.op.stream;

import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.comms.api.DestinationSelector;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.dfw.DataFlowPartition;
import edu.iu.dsc.tws.comms.dfw.io.partition.PartitionPartialReceiver;
import edu.iu.dsc.tws.comms.op.Communicator;

/**
 * Streaming Partition Operation
 */
public class SPartition {
  private static final Logger LOG = Logger.getLogger(SPartition.class.getName());

  /**
   * The actual operation
   */
  private DataFlowPartition partition;

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
  public SPartition(Communicator comm, TaskPlan plan,
                    Set<Integer> sources, Set<Integer> targets, MessageType dataType,
                    MessageReceiver rcvr,
                    DestinationSelector destSelector) {
    this.destinationSelector = destSelector;
    this.partition = new DataFlowPartition(comm.getChannel(), sources, targets, rcvr,
        new PartitionPartialReceiver(), DataFlowPartition.PartitionStratergy.DIRECT, dataType);
    this.partition.init(comm.getConfig(), dataType, plan, comm.nextEdge());
    this.destinationSelector.prepare(partition.getSources(), partition.getDestinations());
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

    boolean send = partition.send(src, message, flags, dest);
    if (send) {
      destinationSelector.commit(src, dest);
    }
    return send;
  }

  /**
   * Weather we have messages pending
   * @return true if there are messages pending
   */
  public boolean hasPending() {
    return !partition.isComplete();
  }

  /**
   * Progress the operation, if not called, messages will not be processed
   *
   * @return true if further progress is needed
   */
  public boolean progress() {
    return partition.progress();
  }

  /**
   * Indicate the end of the communication
   * @param src the source that is ending
   */
  public void finish(int src) {
    partition.finish(src);
  }

  public void close() {
    // deregister from the channel
  }
}
