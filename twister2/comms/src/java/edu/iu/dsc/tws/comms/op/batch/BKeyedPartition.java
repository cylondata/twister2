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

import java.util.Comparator;
import java.util.Set;

import edu.iu.dsc.tws.comms.api.BatchReceiver;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.dfw.DataFlowPartition;
import edu.iu.dsc.tws.comms.dfw.io.KeyedContent;
import edu.iu.dsc.tws.comms.dfw.io.partition.PartitionBatchFinalReceiver;
import edu.iu.dsc.tws.comms.dfw.io.partition.PartitionPartialReceiver;
import edu.iu.dsc.tws.comms.op.Communicator;
import edu.iu.dsc.tws.comms.op.DestinationSelector;

public class BKeyedPartition {
  private DataFlowPartition partition;

  private DestinationSelector destinationSelector;

  public BKeyedPartition(Communicator comm, TaskPlan plan,
                         Set<Integer> sources, Set<Integer> destinations,
                         MessageType dataType, MessageType keyType,
                         BatchReceiver rcvr, DestinationSelector destSelector) {
    this.destinationSelector = destSelector;
    this.partition = new DataFlowPartition(comm.getChannel(), sources, destinations,
        new PartitionBatchFinalReceiver(rcvr, false, true, null),
        new PartitionPartialReceiver(),
        DataFlowPartition.PartitionStratergy.DIRECT, dataType, keyType);
    this.partition.init(comm.getConfig(), dataType, plan, comm.nextEdge());
    this.destinationSelector.prepare(partition.getSources(), partition.getDestinations());
  }

  public BKeyedPartition(Communicator comm, TaskPlan plan,
                         Set<Integer> sources, Set<Integer> destinations, MessageType dataType,
                         MessageType keyType, BatchReceiver rcvr,
                         DestinationSelector destSelector, Comparator<Object> comparator) {
    this.destinationSelector = destSelector;
    this.partition = new DataFlowPartition(comm.getChannel(), sources, destinations,
        new PartitionBatchFinalReceiver(rcvr, true, true, comparator),
        new PartitionPartialReceiver(),
        DataFlowPartition.PartitionStratergy.DIRECT, dataType, keyType);
    this.partition.init(comm.getConfig(), dataType, plan, comm.nextEdge());
    this.destinationSelector.prepare(partition.getSources(), partition.getDestinations());
  }

  public void partition(int source, Object key, Object message, int flags) {
    int destinations = destinationSelector.next(source);

    partition.send(source, new KeyedContent(key, message), flags, destinations);
  }

  public void finish(int source) {
    partition.finish(source);
  }
}
