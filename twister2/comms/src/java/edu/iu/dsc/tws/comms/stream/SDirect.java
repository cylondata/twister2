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
package edu.iu.dsc.tws.comms.stream;

import java.util.List;

import edu.iu.dsc.tws.api.comms.Communicator;
import edu.iu.dsc.tws.api.comms.LogicalPlan;
import edu.iu.dsc.tws.api.comms.SingularReceiver;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageType;
import edu.iu.dsc.tws.comms.dfw.OneToOne;
import edu.iu.dsc.tws.comms.dfw.io.direct.DirectStreamingFinalReceiver;

public class SDirect {
  /**
   * The actual operation
   */
  private OneToOne direct;

  /**
   * Construct a Streaming partition operation
   *
   * @param comm the communicator
   * @param plan task plan
   * @param sources source tasks
   * @param targets target tasks
   * @param rcvr receiver
   * @param dataType data type
   */
  public SDirect(Communicator comm, LogicalPlan plan,
                 List<Integer> sources, List<Integer> targets, MessageType dataType,
                 SingularReceiver rcvr, int edgeId) {
    direct = new OneToOne(comm.getChannel(), sources, targets,
        new DirectStreamingFinalReceiver(rcvr), comm.getConfig(),
        dataType, plan, edgeId);
  }

  public SDirect(Communicator comm, LogicalPlan plan,
                 List<Integer> sources, List<Integer> targets, MessageType dataType,
                 SingularReceiver rcvr) {
    this(comm, plan, sources, targets, dataType, rcvr, comm.nextEdge());
  }

  /**
   * Send a message to be partitioned
   *
   * @param src source
   * @param message message
   * @param flags message flag
   * @return true if the message is accepted
   */
  public boolean partition(int src, Object message, int flags) {
    return direct.send(src, message, flags);
  }

  /**
   * Weather we have messages pending
   *
   * @return true if there are messages pending
   */
  public boolean hasPending() {
    return !direct.isComplete();
  }

  /**
   * Progress the operation, if not called, messages will not be processed
   *
   * @return true if further progress is needed
   */
  public boolean progress() {
    return direct.progress();
  }

  /**
   * Indicate the end of the communication
   *
   * @param src the source that is ending
   */
  public void finish(int src) {
    direct.finish(src);
  }

  public void close() {
    // deregister from the channel
    direct.close();
  }

  /**
   * Clean the operation, this doesn't close it
   */
  public void reset() {
    direct.reset();
  }
}
