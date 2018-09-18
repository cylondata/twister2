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
import edu.iu.dsc.tws.comms.dfw.DataFlowBroadcast;
import edu.iu.dsc.tws.comms.op.Communicator;

/**
 * Streaming Broadcast Operation
 */
public class SBroadCast {
  private DataFlowBroadcast bCast;

  /**
   * Construct a Streaming Broadcast operation
   * @param comm the communicator
   * @param plan task plan
   * @param source source task
   * @param targets target tasks
   * @param rcvr receiver
   * @param dataType data type
   */
  public SBroadCast(Communicator comm, TaskPlan plan,
                    int source, Set<Integer> targets, MessageType dataType,
                    MessageReceiver rcvr) {
    this.bCast = new DataFlowBroadcast(comm.getChannel(), source, targets, rcvr);
    this.bCast.init(comm.getConfig(), dataType, plan, comm.nextEdge());
  }

  /**
   * Send a message to be reduced
   *
   * @param source source
   * @param message message
   * @param flags message flag
   * @return true if the message is accepted
   */
  public boolean bcast(int source, Object message, int flags) {
    return bCast.send(source, message, flags);
  }

  /**
   * Progress the operation, if not called, messages will not be processed
   *
   * @return true if further progress is needed
   */
  public boolean progress() {
    return bCast.progress();
  }

  /**
   * Weather we have messages pending
   * @return true if there are messages pending
   */
  public boolean hasPending() {
    return !bCast.isComplete();
  }
}
