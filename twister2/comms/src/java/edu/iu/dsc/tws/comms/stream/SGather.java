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
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.comms.BaseOperation;
import edu.iu.dsc.tws.api.comms.BulkReceiver;
import edu.iu.dsc.tws.api.comms.CommunicationContext;
import edu.iu.dsc.tws.api.comms.Communicator;
import edu.iu.dsc.tws.api.comms.LogicalPlan;
import edu.iu.dsc.tws.api.comms.messaging.MessageFlags;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageType;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.comms.packing.MessageSchema;
import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.comms.dfw.MToOneTree;
import edu.iu.dsc.tws.comms.dfw.io.gather.GatherStreamingFinalReceiver;
import edu.iu.dsc.tws.comms.dfw.io.gather.GatherStreamingPartialReceiver;
import edu.iu.dsc.tws.comms.utils.LogicalPlanBuilder;

/**
 * Streaming Gather Operation
 */
public class SGather extends BaseOperation {
  private static final Logger LOG = Logger.getLogger(SReduce.class.getName());

  /**
   * The data type
   */
  private MessageType dataType;

  /**
   * Construct a Streaming Gather operation
   *
   * @param comm the communicator
   * @param plan task plan
   * @param sources source tasks
   * @param target target tasks
   * @param rcvr receiver
   * @param dataType data type
   */
  public SGather(Communicator comm, LogicalPlan plan,
                 Set<Integer> sources, int target, MessageType dataType,
                 BulkReceiver rcvr, int edgeId, MessageSchema messageSchema) {
    super(comm, true, CommunicationContext.GATHER);
    this.dataType = dataType;
    MToOneTree gather = new MToOneTree(comm.getChannel(), sources, target,
        new GatherStreamingFinalReceiver(rcvr),
        new GatherStreamingPartialReceiver(), 0, 0,
        true, MessageTypes.INTEGER, dataType, messageSchema);
    gather.init(comm.getConfig(), dataType, plan, edgeId);
    op = gather;
  }

  public SGather(Communicator comm, LogicalPlan plan,
                 Set<Integer> sources, int target, MessageType dataType,
                 BulkReceiver rcvr) {
    this(comm, plan, sources, target, dataType, rcvr, comm.nextEdge(), MessageSchema.noSchema());
  }

  public SGather(Communicator comm, LogicalPlanBuilder logicalPlanBuilder, MessageType dataType,
                 BulkReceiver rcvr) {
    this(comm, logicalPlanBuilder.build(),
        logicalPlanBuilder.getSources(),
        logicalPlanBuilder.getTargets().iterator().next(),
        dataType, rcvr, comm.nextEdge(), MessageSchema.noSchema());
  }

  /**
   * Send a message to be gathered
   *
   * @param src source
   * @param message message
   * @param flags message flag
   * @return true if the message is accepted
   */
  public boolean gather(int src, Object message, int flags) {
    if ((flags & MessageFlags.SYNC_BARRIER) == MessageFlags.SYNC_BARRIER) {
      return op.send(src, message, flags);
    } else {
      Tuple tuple = new Tuple<>(src, message);
      return op.send(src, tuple, flags);
    }
  }
}
