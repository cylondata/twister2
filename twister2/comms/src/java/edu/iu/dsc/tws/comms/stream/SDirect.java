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

import java.util.ArrayList;
import java.util.List;

import edu.iu.dsc.tws.api.comms.CommunicationContext;
import edu.iu.dsc.tws.api.comms.Communicator;
import edu.iu.dsc.tws.api.comms.LogicalPlan;
import edu.iu.dsc.tws.api.comms.SingularReceiver;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageType;
import edu.iu.dsc.tws.api.comms.packing.MessageSchema;
import edu.iu.dsc.tws.comms.dfw.BaseOperation;
import edu.iu.dsc.tws.comms.dfw.OneToOne;
import edu.iu.dsc.tws.comms.dfw.io.direct.DirectStreamingFinalReceiver;
import edu.iu.dsc.tws.comms.utils.LogicalPlanBuilder;

public class SDirect extends BaseOperation {

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
                 SingularReceiver rcvr, int edgeId, MessageSchema messageSchema) {
    super(comm, true, CommunicationContext.DIRECT);
    op = new OneToOne(comm.getChannel(), sources, targets,
        new DirectStreamingFinalReceiver(rcvr), comm.getConfig(),
        dataType, plan, edgeId, messageSchema);
  }

  public SDirect(Communicator comm, LogicalPlan plan,
                 List<Integer> sources, List<Integer> targets, MessageType dataType,
                 SingularReceiver rcvr) {
    this(comm, plan, sources, targets, dataType, rcvr, comm.nextEdge(), MessageSchema.noSchema());
  }

  public SDirect(Communicator comm, LogicalPlanBuilder logicalPlanBuilder, MessageType dataType,
                 SingularReceiver rcvr) {
    this(comm, logicalPlanBuilder.build(),
        new ArrayList<>(logicalPlanBuilder.getSources()),
        new ArrayList<>(logicalPlanBuilder.getTargets()),
        dataType, rcvr, comm.nextEdge(), MessageSchema.noSchema());
  }

  public SDirect(Communicator comm, LogicalPlan plan,
                 List<Integer> sources, List<Integer> targets, MessageType dataType,
                 SingularReceiver rcvr, MessageSchema messageSchema) {
    this(comm, plan, sources, targets, dataType, rcvr, comm.nextEdge(), messageSchema);
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
    return op.send(src, message, flags);
  }
}
