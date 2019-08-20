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

import java.util.Set;

import edu.iu.dsc.tws.api.comms.CommunicationContext;
import edu.iu.dsc.tws.api.comms.Communicator;
import edu.iu.dsc.tws.api.comms.LogicalPlan;
import edu.iu.dsc.tws.api.comms.SingularReceiver;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageType;
import edu.iu.dsc.tws.api.comms.packing.MessageSchema;
import edu.iu.dsc.tws.comms.dfw.BaseOperation;
import edu.iu.dsc.tws.comms.dfw.TreeBroadcast;
import edu.iu.dsc.tws.comms.dfw.io.direct.DirectStreamingFinalReceiver;
import edu.iu.dsc.tws.comms.utils.LogicalPlanBuilder;

/**
 * Streaming Broadcast Operation
 */
public class SBroadCast extends BaseOperation {
  /**
   * Construct a Streaming Broadcast operation
   *
   * @param comm the communicator
   * @param plan task plan
   * @param source source task
   * @param targets target tasks
   * @param rcvr receiver
   * @param dataType data type
   */
  public SBroadCast(Communicator comm, LogicalPlan plan,
                    int source, Set<Integer> targets, MessageType dataType,
                    SingularReceiver rcvr, int edgeId, MessageSchema messageSchema) {
    super(comm, true, CommunicationContext.BROADCAST);
    TreeBroadcast bCast = new TreeBroadcast(comm.getChannel(), source, targets,
        new DirectStreamingFinalReceiver(rcvr), messageSchema);
    bCast.init(comm.getConfig(), dataType, plan, edgeId);
    op = bCast;
  }

  public SBroadCast(Communicator comm, LogicalPlan plan,
                    int source, Set<Integer> targets, MessageType dataType,
                    SingularReceiver rcvr) {
    this(comm, plan, source, targets, dataType, rcvr, comm.nextEdge(), MessageSchema.noSchema());
  }

  public SBroadCast(Communicator comm, LogicalPlanBuilder logicalPlanBuilder, MessageType dataType,
                    SingularReceiver rcvr) {
    this(comm, logicalPlanBuilder.build(),
        logicalPlanBuilder.getSources().iterator().next(),
        logicalPlanBuilder.getTargets(), dataType, rcvr, comm.nextEdge(), MessageSchema.noSchema());
  }

  public SBroadCast(Communicator comm, LogicalPlan plan,
                    int source, Set<Integer> targets, MessageType dataType,
                    SingularReceiver rcvr, MessageSchema messageSchema) {
    this(comm, plan, source, targets, dataType, rcvr, comm.nextEdge(), messageSchema);
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
    return op.send(source, message, flags);
  }
}
