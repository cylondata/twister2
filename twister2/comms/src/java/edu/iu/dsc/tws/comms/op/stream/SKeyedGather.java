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

import edu.iu.dsc.tws.comms.api.BulkReceiver;
import edu.iu.dsc.tws.comms.api.DestinationSelector;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.dfw.DataFlowMultiGather;
import edu.iu.dsc.tws.comms.dfw.io.KeyedContent;
import edu.iu.dsc.tws.comms.dfw.io.gather.GatherMultiStreamingFinalReceiver;
import edu.iu.dsc.tws.comms.dfw.io.gather.GatherMultiStreamingPartialReceiver;
import edu.iu.dsc.tws.comms.op.Communicator;

/**
 * Streaming Keyed Partition Operation
 */
public class SKeyedGather {
  /**
   * The actual operation
   */
  private DataFlowMultiGather keyedGather;

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
   * @param rcvr receiver
   * @param destSelector destination selector
   */
  public SKeyedGather(Communicator comm, TaskPlan plan,
                      Set<Integer> sources, Set<Integer> targets, MessageType kType,
                      MessageType dType, BulkReceiver rcvr,
                      DestinationSelector destSelector) {
    this.keyType = kType;
    this.dataType = dType;

    Set<Integer> edges = new HashSet<>();
    for (int i = 0; i < targets.size(); i++) {
      edges.add(comm.nextEdge());
    }

    this.keyedGather = new DataFlowMultiGather(comm.getChannel(), sources, targets,
        new GatherMultiStreamingFinalReceiver(rcvr),
        new GatherMultiStreamingPartialReceiver(), edges, keyType, dataType);
    this.keyedGather.init(comm.getConfig(), dataType, plan);
    this.destinationSelector = destSelector;
    this.destinationSelector.prepare(comm, sources, targets);

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
    int dest = destinationSelector.next(src, key, message);
    return keyedGather.send(src, new KeyedContent(key, message, keyType, dataType), flags, dest);
  }

  /**
   * Weather we have messages pending
   *
   * @return true if there are messages pending
   */
  public boolean hasPending() {
    return !keyedGather.isComplete();
  }

  /**
   * Indicate the end of the communication
   *
   * @param src the source that is ending
   */
  public void finish(int src) {
    keyedGather.finish(src);
  }

  /**
   * Progress the operation, if not called, messages will not be processed
   *
   * @return true if further progress is needed
   */
  public boolean progress() {
    return keyedGather.progress();
  }
}
