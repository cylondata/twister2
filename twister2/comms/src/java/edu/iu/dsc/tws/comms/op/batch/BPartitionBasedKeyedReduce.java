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
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.api.ReduceFunction;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.dfw.DataFlowPartition;
import edu.iu.dsc.tws.comms.dfw.io.KeyedContent;
import edu.iu.dsc.tws.comms.dfw.io.reduce.keyed.KReduceBatchFinalReceiver;
import edu.iu.dsc.tws.comms.dfw.io.reduce.keyed.PartitionBasedReducePartialReceiver;
import edu.iu.dsc.tws.comms.op.Communicator;

public class BPartitionBasedKeyedReduce {

  private DataFlowPartition partition;

  private DestinationSelector destinationSelector;

  public BPartitionBasedKeyedReduce(Communicator comm, TaskPlan plan,
                                    Set<Integer> sources, Set<Integer> destinations,
                                    MessageType dataType, MessageType keyType,
                                    BulkReceiver rcvr, DestinationSelector destSelector,
                                    ReduceFunction reduceFunction) {
    this.destinationSelector = destSelector;
    this.partition = new DataFlowPartition(comm.getChannel(), sources, destinations,
        new KReduceBatchFinalReceiver(reduceFunction, rcvr),
        new PartitionBasedReducePartialReceiver(reduceFunction),
        DataFlowPartition.PartitionStratergy.DIRECT, dataType, keyType);
    this.partition.init(comm.getConfig(), dataType, plan, comm.nextEdge());
    this.destinationSelector.prepare(partition.getSources(), partition.getDestinations());
  }

  public boolean partition(int source, Object key, Object message, int flags) {
    int destinations = destinationSelector.next(source, key, message);

    return partition.send(source, new KeyedContent(key, message, partition.getKeyType(),
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

}
