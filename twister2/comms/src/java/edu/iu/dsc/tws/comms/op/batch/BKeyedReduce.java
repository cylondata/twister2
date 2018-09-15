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

import java.util.HashSet;
import java.util.Set;

import edu.iu.dsc.tws.comms.api.DestinationSelector;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.api.ReduceFunction;
import edu.iu.dsc.tws.comms.api.SingularReceiver;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.dfw.DataFlowMultiReduce;
import edu.iu.dsc.tws.comms.dfw.io.KeyedContent;
import edu.iu.dsc.tws.comms.dfw.io.reduce.ReduceMultiBatchFinalReceiver;
import edu.iu.dsc.tws.comms.dfw.io.reduce.ReduceMultiBatchPartialReceiver;
import edu.iu.dsc.tws.comms.op.Communicator;

/**
 * Example class for Batch keyed reduce. The reduce destination for each data point will be
 * based on the key value related to that data point.
 */
public class BKeyedReduce {
  private DataFlowMultiReduce keyedReduce;

  private DestinationSelector destinationSelector;

  private MessageType keyType;

  private MessageType dataType;

  public BKeyedReduce(Communicator comm, TaskPlan plan,
                      Set<Integer> sources, Set<Integer> destinations, ReduceFunction fnc,
                      SingularReceiver rcvr, MessageType kType, MessageType dType,
                      DestinationSelector destSelector) {
    this.keyType = kType;
    this.dataType = dType;
    Set<Integer> edges = new HashSet<>();
    for (int i = 0; i < destinations.size(); i++) {
      edges.add(comm.nextEdge());
    }
    this.keyedReduce = new DataFlowMultiReduce(comm.getChannel(), sources, destinations,
        new ReduceMultiBatchFinalReceiver(fnc, rcvr),
        new ReduceMultiBatchPartialReceiver(fnc), edges, keyType, dataType);
    this.keyedReduce.init(comm.getConfig(), dataType, plan);
    this.destinationSelector = destSelector;
    this.destinationSelector.prepare(sources, destinations);

  }

  public boolean reduce(int src, Object key, Object data, int flags) {
    int dest = destinationSelector.next(src, key);
    return keyedReduce.send(src, new KeyedContent(key, data, keyType,
        dataType), flags, dest);
  }

  public boolean hasPending() {
    return !keyedReduce.isComplete();
  }

  public void finish(int src) {
    keyedReduce.finish(src);
  }

  public boolean progress() {
    return keyedReduce.progress();
  }
}
