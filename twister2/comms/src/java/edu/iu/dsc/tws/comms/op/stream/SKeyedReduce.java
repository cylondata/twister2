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

import java.util.HashSet;
import java.util.Set;

import edu.iu.dsc.tws.comms.api.DestinationSelector;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.api.ReduceFunction;
import edu.iu.dsc.tws.comms.api.SingularReceiver;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.dfw.DataFlowMultiReduce;
import edu.iu.dsc.tws.comms.dfw.io.KeyedContent;
import edu.iu.dsc.tws.comms.dfw.io.reduce.ReduceMultiStreamingFinalReceiver;
import edu.iu.dsc.tws.comms.dfw.io.reduce.ReduceMultiStreamingPartialReceiver;
import edu.iu.dsc.tws.comms.op.Communicator;

/**
 * Streaming Keyed Partition Operation
 */
public class SKeyedReduce {
  /**
   * The actual operation
   */
  private DataFlowMultiReduce keyedReduce;

  /**
   * Destination selector
   */
  private DestinationSelector destinationSelector;

  /**
   * Key type
   */
  private MessageType keyType;

  /**
   * Data type
   */
  private MessageType dataType;

  /**
   * Construct a Streaming Key based partition operation
   *
   * @param comm the communicator
   * @param plan task plan
   * @param sources source tasks
   * @param targets target tasks
   * @param dType data type
   * @param kType key type
   * @param fnc reduce function
   * @param rcvr receiver
   * @param destSelector destination selector
   */
  public SKeyedReduce(Communicator comm, TaskPlan plan,
                      Set<Integer> sources, Set<Integer> targets, MessageType kType,
                      MessageType dType, ReduceFunction fnc, SingularReceiver rcvr,
                      DestinationSelector destSelector) {
    this.keyType = kType;
    this.dataType = dType;

    Set<Integer> edges = new HashSet<>();
    for (int i = 0; i < targets.size(); i++) {
      edges.add(comm.nextEdge());
    }

    this.keyedReduce = new DataFlowMultiReduce(comm.getChannel(), sources, targets,
        new ReduceMultiStreamingFinalReceiver(fnc, rcvr),
        new ReduceMultiStreamingPartialReceiver(fnc), edges, keyType, dataType);
    this.keyedReduce.init(comm.getConfig(), dataType, plan);
    this.destinationSelector = destSelector;
    this.destinationSelector.prepare(keyType, sources, targets);

  }

  /**
   * Send a message to be reduced
   *
   * @param src source
   * @param key key
   * @param message message
   * @param flags message flag
   * @return true if the message is accepted
   */
  public boolean reduce(int src, Object key, Object message, int flags) {
    int dest = destinationSelector.next(src, key);
    return keyedReduce.send(src, new KeyedContent(key, message, keyType, dataType), flags, dest);
  }

  /**
   * Weather we have messages pending
   * @return true if there are messages pending
   */
  public boolean hasPending() {
    return !keyedReduce.isComplete();
  }

  /**
   * Indicate the end of the communication
   * @param src the source that is ending
   */
  public void finish(int src) {
    keyedReduce.finish(src);
  }

  /**
   * Progress the operation, if not called, messages will not be processed
   *
   * @return true if further progress is needed
   */
  public boolean progress() {
    return keyedReduce.progress();
  }
}
