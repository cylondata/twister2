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

import java.util.Comparator;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.comms.BulkReceiver;
import edu.iu.dsc.tws.api.comms.CommunicationContext;
import edu.iu.dsc.tws.api.comms.Communicator;
import edu.iu.dsc.tws.api.comms.DataFlowOperation;
import edu.iu.dsc.tws.api.comms.DestinationSelector;
import edu.iu.dsc.tws.api.comms.LogicalPlan;
import edu.iu.dsc.tws.api.comms.messaging.MessageReceiver;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageType;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.comms.dfw.MToNRing;
import edu.iu.dsc.tws.comms.dfw.MToNSimple;
import edu.iu.dsc.tws.comms.dfw.io.gather.keyed.KGatherBatchFinalReceiver;
import edu.iu.dsc.tws.comms.dfw.io.partition.DPartitionBatchFinalReceiver;
import edu.iu.dsc.tws.comms.dfw.io.partition.PartitionPartialReceiver;

public class BKeyedGather {

  private static final Logger LOG = Logger.getLogger(BKeyedGather.class.getName());

  private DataFlowOperation keyedGather;

  private DestinationSelector destinationSelector;

  private MessageType keyType;

  private MessageType dataType;

  /**
   * Creates an instance of BKeyedGather without shuffling
   */
  public BKeyedGather(Communicator comm, LogicalPlan plan,
                      Set<Integer> sources, Set<Integer> destinations,
                      MessageType kType, MessageType dType,
                      BulkReceiver rcvr, DestinationSelector destSelector) {
    this(comm, plan, sources, destinations, kType, dType, rcvr,
        destSelector, false, null, true);
  }

  public BKeyedGather(Communicator comm, LogicalPlan plan,
                      Set<Integer> sources, Set<Integer> destinations,
                      MessageType kType, MessageType dType, BulkReceiver rcvr,
                      DestinationSelector destSelector,
                      boolean useDisk,
                      Comparator<Object> comparator,
                      boolean groupByKey) {
    this(comm, plan, sources, destinations, kType, dType, rcvr, destSelector,
        useDisk, comparator, groupByKey, comm.nextEdge());
  }

  /**
   * Creates an instance of BKeyedGather with key comparator
   *
   * @param comparator Comparator to use for sorting keys
   */
  public BKeyedGather(Communicator comm, LogicalPlan plan,
                      Set<Integer> sources, Set<Integer> destinations,
                      MessageType kType, MessageType dType, BulkReceiver rcvr,
                      DestinationSelector destSelector,
                      boolean useDisk,
                      Comparator<Object> comparator,
                      boolean groupByKey, int edgeId) {
    if (useDisk && comparator == null) {
      throw new RuntimeException("Key comparator should be specified in disk based mode");
    }
    this.keyType = kType;
    this.dataType = dType;

    MessageType receiveDataType = dataType;
    MessageReceiver finalReceiver;
    MessageReceiver partialReceiver = new PartitionPartialReceiver();
    if (!useDisk) {
      finalReceiver = new KGatherBatchFinalReceiver(rcvr, groupByKey);
    } else {
      receiveDataType = MessageTypes.BYTE_ARRAY;
      finalReceiver = new DPartitionBatchFinalReceiver(
          rcvr, true, comm.getPersistentDirectories(), comparator, groupByKey);
    }

    if (CommunicationContext.TWISTER2_PARTITION_ALGO_SIMPLE.equals(
        CommunicationContext.partitionBatchAlgorithm(comm.getConfig()))) {
      this.keyedGather = new MToNSimple(comm.getConfig(), comm.getChannel(),
          plan, sources, destinations,
          finalReceiver, partialReceiver, dataType, receiveDataType,
          keyType, keyType, edgeId);
    } else if (CommunicationContext.TWISTER2_PARTITION_ALGO_RING.equals(
        CommunicationContext.partitionBatchAlgorithm(comm.getConfig()))) {
      this.keyedGather = new MToNRing(comm.getConfig(), comm.getChannel(),
          plan, sources, destinations, finalReceiver, partialReceiver,
          dataType, receiveDataType, keyType, keyType, edgeId);
    }

    this.destinationSelector = destSelector;
    this.destinationSelector.prepare(comm, sources, destinations);
  }

  public boolean gather(int source, Object key, Object data, int flags) {
    int dest = destinationSelector.next(source, key, data);
    return keyedGather.send(source, Tuple.of(key, data, keyType,
        dataType), flags, dest);
  }

  public boolean hasPending() {
    return !keyedGather.isComplete();
  }

  public void finish(int source) {
    keyedGather.finish(source);
  }

  public boolean progress() {
    return keyedGather.progress();
  }

  public void close() {
    // deregister from the channel
    keyedGather.close();
  }

  /**
   * Clean the operation, this doesn't close it
   */
  public void reset() {
    keyedGather.reset();
  }
}
