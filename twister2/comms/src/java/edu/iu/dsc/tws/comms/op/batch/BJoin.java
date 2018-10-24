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

package edu.iu.dsc.tws.comms.op.batch;

import java.util.Set;

import edu.iu.dsc.tws.comms.api.BulkReceiver;
import edu.iu.dsc.tws.comms.api.DestinationSelector;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.dfw.DataFlowPartition;
import edu.iu.dsc.tws.comms.dfw.io.KeyedContent;
import edu.iu.dsc.tws.comms.dfw.io.join.JoinBatchFinalReceiver;
import edu.iu.dsc.tws.comms.dfw.io.join.JoinBatchPartialReceiver;
import edu.iu.dsc.tws.comms.dfw.io.partition.DPartitionBatchFinalReceiver;
import edu.iu.dsc.tws.comms.dfw.io.partition.PartitionPartialReceiver;
import edu.iu.dsc.tws.comms.op.Communicator;

/**
 * Batch Join operation
 */
public class BJoin {
  /**
   * Left partition of the join
   */
  private DataFlowPartition partitionLeft;

  /**
   * Right partition of the join
   */
  private DataFlowPartition partitionRight;

  /**
   * Destination selector
   */
  private DestinationSelector destinationSelector;

  /**
   * Construct a Batch partition operation
   *
   * @param comm the communicator
   * @param plan task plan
   * @param sources source tasks
   * @param targets target tasks
   * @param rcvr receiver
   * @param dataType data type
   * @param destSelector destination selector
   */
  public BJoin(Communicator comm, TaskPlan plan,
               Set<Integer> sources, Set<Integer> targets, MessageType keyType,
               MessageType dataType, BulkReceiver rcvr,
               DestinationSelector destSelector, boolean shuffle) {
    this.destinationSelector = destSelector;
    String shuffleDir = comm.getPersistentDirectory();

    MessageReceiver finalRcvr;
    if (shuffle) {
      finalRcvr = new DPartitionBatchFinalReceiver(
          rcvr, false, shuffleDir, null);
    } else {
      finalRcvr = new JoinBatchFinalReceiver(rcvr);
    }


    this.partitionLeft = new DataFlowPartition(comm.getChannel(), sources, targets,
        new JoinBatchPartialReceiver(0, finalRcvr), new PartitionPartialReceiver(),
        DataFlowPartition.PartitionStratergy.DIRECT, dataType, keyType);

    this.partitionRight = new DataFlowPartition(comm.getChannel(), sources, targets,
        new JoinBatchPartialReceiver(1, finalRcvr), new PartitionPartialReceiver(),
        DataFlowPartition.PartitionStratergy.DIRECT, dataType, keyType);

    this.partitionLeft.init(comm.getConfig(), dataType, plan, comm.nextEdge());
    this.partitionRight.init(comm.getConfig(), dataType, plan, comm.nextEdge());
    this.destinationSelector.prepare(sources, targets);
  }

  /**
   * Send a data to be partitioned
   *
   * @param source source
   * @param key key for the data
   * @param data data
   * @param flags data flag
   * @return true if the data is accepted
   */
  public boolean partition(int source, Object key, Object data, int flags, int tag) {
    int dest = destinationSelector.next(source, key, data);

    boolean send;
    KeyedContent message = new KeyedContent(key, data, partitionLeft.getKeyType(),
        partitionLeft.getDataType());
    if (tag == 0) {
      send = partitionLeft.send(source, message, flags, dest);
    } else if (tag == 1) {
      send = partitionRight.send(source, message, flags, dest);
    } else {
      throw new RuntimeException("Tag value must be either 0(left) or 1(right) for join operation");
    }

    if (send) {
      destinationSelector.commit(source, dest);
    }
    return send;
  }

  /**
   * Weather we have messages pending
   *
   * @return true if there are messages pending
   */
  public boolean hasPending() {
    return !(partitionLeft.isComplete() && partitionRight.isComplete());
  }

  /**
   * Indicate the end of the communication for a given tag value
   *
   * @param source the source that is ending
   */
  public void finish(int source, int tag) {
    if (tag == 0) {
      partitionLeft.finish(source);
    } else if (tag == 1) {
      partitionRight.finish(source);
    } else {
      throw new RuntimeException("Tag value must be either 0(left) or 1(right) for join operation");
    }
  }

  /**
   * Progress the operation, if not called, messages will not be processed
   *
   * @return true if further progress is needed
   */
  public boolean progress() {
    return partitionLeft.progress() && partitionRight.progress();
  }

  public void close() {
    partitionLeft.close();
    partitionRight.close();
  }
}
