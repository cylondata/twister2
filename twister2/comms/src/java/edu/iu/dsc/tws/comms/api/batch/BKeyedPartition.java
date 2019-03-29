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
import java.util.Set;

import edu.iu.dsc.tws.comms.api.BulkReceiver;
import edu.iu.dsc.tws.comms.api.Communicator;
import edu.iu.dsc.tws.comms.api.DestinationSelector;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.api.TaskPlan;
import edu.iu.dsc.tws.comms.dfw.DataFlowPartition;
import edu.iu.dsc.tws.comms.dfw.io.Tuple;
import edu.iu.dsc.tws.comms.dfw.io.partition.DPartitionBatchFinalReceiver;
import edu.iu.dsc.tws.comms.dfw.io.partition.PartitionBatchFinalReceiver;
import edu.iu.dsc.tws.comms.dfw.io.partition.PartitionPartialReceiver;

public class BKeyedPartition {
  private DataFlowPartition partition;

  private DestinationSelector destinationSelector;

  public BKeyedPartition(Communicator comm, TaskPlan plan,
                         Set<Integer> sources, Set<Integer> destinations,
                         MessageType dataType, MessageType keyType,
                         BulkReceiver rcvr, DestinationSelector destSelector) {
    this.destinationSelector = destSelector;
    this.partition = new DataFlowPartition(comm.getChannel(), sources, destinations,
        new PartitionBatchFinalReceiver(rcvr),
        new PartitionPartialReceiver(), dataType, keyType);
    this.partition.init(comm.getConfig(), dataType, plan, comm.nextEdge());
    this.destinationSelector.prepare(comm, partition.getSources(), partition.getTargets());
  }

  public BKeyedPartition(Communicator comm, TaskPlan plan,
                         Set<Integer> sources, Set<Integer> destinations, MessageType dataType,
                         MessageType keyType, BulkReceiver rcvr,
                         DestinationSelector destSelector, Comparator<Object> comparator) {
    this.destinationSelector = destSelector;
    String shuffleDir = comm.getPersistentDirectory();
    int e = comm.nextEdge();
    this.partition = new DataFlowPartition(comm.getConfig(), comm.getChannel(), plan,
        sources, destinations,
        new DPartitionBatchFinalReceiver(rcvr, true, shuffleDir, comparator),
        new PartitionPartialReceiver(), dataType, MessageType.BYTE, keyType,
        keyType, e);
    this.partition.init(comm.getConfig(), dataType, plan, e);
    this.destinationSelector.prepare(comm, partition.getSources(), partition.getTargets());
  }

  public boolean partition(int source, Object key, Object message, int flags) {
    int destinations = destinationSelector.next(source, key, message);

    return partition.send(source, new Tuple(key, message, partition.getKeyType(),
        partition.getDataType()), flags, destinations);
  }

  public boolean hasPending() {
    return !partition.isComplete();
  }

  public void finish(int source) {
    partition.finish(source);
  }

  public boolean progress() {
    return partition.progress();
  }

  public void close() {
    // deregister from the channel
    partition.close();
  }
}
