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

package edu.iu.dsc.tws.comms.api.batch;

import java.util.Comparator;
import java.util.List;
import java.util.Set;

import edu.iu.dsc.tws.comms.api.BulkReceiver;
import edu.iu.dsc.tws.comms.api.Communicator;
import edu.iu.dsc.tws.comms.api.DestinationSelector;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.api.TaskPlan;
import edu.iu.dsc.tws.comms.dfw.MToNSimple;
import edu.iu.dsc.tws.comms.dfw.io.Tuple;
import edu.iu.dsc.tws.comms.dfw.io.join.DJoinBatchFinalReceiver2;
import edu.iu.dsc.tws.comms.dfw.io.join.JoinBatchFinalReceiver;
import edu.iu.dsc.tws.comms.dfw.io.join.JoinBatchPartialReceiver;
import edu.iu.dsc.tws.comms.dfw.io.partition.PartitionPartialReceiver;

/**
 * Batch Join operation
 */
public class BJoin {
  /**
   * Left partition of the join
   */
  private MToNSimple partitionLeft;

  /**
   * Right partition of the join
   */
  private MToNSimple partitionRight;

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
   * @param leftDataType data type
   * @param destSelector destination selector
   */
  public BJoin(Communicator comm, TaskPlan plan,
               Set<Integer> sources, Set<Integer> targets, MessageType keyType,
               MessageType leftDataType, MessageType rightDataType, BulkReceiver rcvr,
               DestinationSelector destSelector, boolean shuffle, Comparator<Object> comparator) {
    this.destinationSelector = destSelector;
    List<String> shuffleDirs = comm.getPersistentDirectories();

    MessageReceiver finalRcvr;
    if (shuffle) {
      finalRcvr = new DJoinBatchFinalReceiver2(rcvr, shuffleDirs, comparator);
    } else {
      finalRcvr = new JoinBatchFinalReceiver(rcvr, comparator);
    }


    this.partitionLeft = new MToNSimple(comm.getChannel(), sources, targets,
        new JoinBatchPartialReceiver(0, finalRcvr), new PartitionPartialReceiver(),
        leftDataType, keyType);

    this.partitionRight = new MToNSimple(comm.getChannel(), sources, targets,
        new JoinBatchPartialReceiver(1, finalRcvr), new PartitionPartialReceiver(),
        rightDataType, keyType);

    this.partitionLeft.init(comm.getConfig(), leftDataType, plan, comm.nextEdge());
    this.partitionRight.init(comm.getConfig(), rightDataType, plan, comm.nextEdge());
    this.destinationSelector.prepare(comm, sources, targets);
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
  public boolean join(int source, Object key, Object data, int flags, int tag) {
    int dest = destinationSelector.next(source, key, data);

    boolean send;
    Tuple message = new Tuple(key, data, partitionLeft.getKeyType(),
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
  public void finish(int source) {
    partitionLeft.finish(source);
    partitionRight.finish(source);
  }

  /**
   * Progress the operation, if not called, messages will not be processed
   *
   * @return true if further progress is needed
   */
  public boolean progress() {

    return partitionLeft.progress() | partitionRight.progress();
  }

  public void close() {
    partitionLeft.close();
    partitionRight.close();
  }

  /**
   * Clean the operation, this doesn't close it
   */
  public void reset() {
    partitionLeft.reset();
    partitionRight.reset();
  }

  private class IntegerComparator implements Comparator<Object> {
    @Override
    public int compare(Object o1, Object o2) {
      int o11 = (int) o1;
      int o21 = (int) o2;
      return Integer.compare(o11, o21);
    }
  }
}
