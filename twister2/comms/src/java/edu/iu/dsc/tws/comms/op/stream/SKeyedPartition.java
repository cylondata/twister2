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

import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.dfw.DataFlowPartition;
import edu.iu.dsc.tws.comms.dfw.io.KeyedContent;
import edu.iu.dsc.tws.comms.dfw.io.partition.PartitionPartialReceiver;
import edu.iu.dsc.tws.comms.op.Communicator;
import edu.iu.dsc.tws.comms.op.DestinationSelector;

public class SKeyedPartition {
  private DataFlowPartition partition;

  private Communicator comm;

  private DestinationSelector destinationSelector;

  public SKeyedPartition(Communicator comm, TaskPlan plan,
                         Set<Integer> sources, Set<Integer> destinations, MessageType dataType,
                         MessageType keyType, MessageReceiver rcvr,
                         DestinationSelector destSelector) {
    this.destinationSelector = destSelector;
    this.partition = new DataFlowPartition(comm.getChannel(), sources, destinations, rcvr,
        new PartitionPartialReceiver(),
        DataFlowPartition.PartitionStratergy.DIRECT, dataType, keyType);

    this.partition.init(comm.getConfig(), dataType, plan, comm.nextEdge());
    this.destinationSelector.prepare(partition.getSources(), partition.getDestinations());

    destinationSelector.prepare(keyType, sources, destinations);
  }

  public void partition(int source, Object key, Object message, int flags) {
    int destinations = destinationSelector.next(source, message);

    partition.send(source, new KeyedContent(key, message), flags, destinations);
  }

  public void finish(int source) {
    partition.finish(source);
  }
}
