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
package edu.iu.dsc.tws.comms.batch;

import java.util.Set;

import edu.iu.dsc.tws.api.comms.BulkReceiver;
import edu.iu.dsc.tws.api.comms.CommunicationContext;
import edu.iu.dsc.tws.api.comms.Communicator;
import edu.iu.dsc.tws.api.comms.LogicalPlan;
import edu.iu.dsc.tws.api.comms.messaging.MessageReceiver;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageType;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.comms.packing.MessageSchema;
import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.comms.dfw.BaseOperation;
import edu.iu.dsc.tws.comms.dfw.MToOneTree;
import edu.iu.dsc.tws.comms.dfw.io.gather.DGatherBatchFinalReceiver;
import edu.iu.dsc.tws.comms.dfw.io.gather.GatherBatchFinalReceiver;
import edu.iu.dsc.tws.comms.dfw.io.gather.GatherBatchPartialReceiver;
import edu.iu.dsc.tws.comms.utils.LogicalPlanBuilder;

/**
 * Batch Gather Operation
 */
public class BGather extends BaseOperation {
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
   * @param shuffle weather to use disks
   */
  public BGather(Communicator comm, LogicalPlan plan,
                 Set<Integer> sources, int target,
                 MessageType dataType,
                 BulkReceiver rcvr, boolean shuffle) {
    this(comm, plan, sources, target, dataType, rcvr, shuffle,
        comm.nextEdge(), MessageSchema.noSchema());
  }

  public BGather(Communicator comm, LogicalPlanBuilder logicalPlanBuilder,
                 MessageType dataType,
                 BulkReceiver rcvr, boolean shuffle) {
    this(comm, logicalPlanBuilder.build(), logicalPlanBuilder.getSources(),
        logicalPlanBuilder.getTargets().iterator().next(), dataType, rcvr, shuffle,
        comm.nextEdge(), MessageSchema.noSchema());
  }


  public BGather(Communicator comm, LogicalPlan plan,
                 Set<Integer> sources, int target,
                 MessageType dataType,
                 BulkReceiver rcvr, boolean shuffle, MessageSchema messageSchema) {
    this(comm, plan, sources, target, dataType, rcvr, shuffle,
        comm.nextEdge(), messageSchema);
  }

  public BGather(Communicator comm, LogicalPlan plan,
                 Set<Integer> sources, int target,
                 MessageType dataType,
                 BulkReceiver rcvr, boolean shuffle, int edgeId, MessageSchema messageSchema) {
    super(comm, false, CommunicationContext.GATHER);
    MessageReceiver finalRcvr;
    if (!shuffle) {
      finalRcvr = new GatherBatchFinalReceiver(rcvr);
    } else {
      finalRcvr = new DGatherBatchFinalReceiver(rcvr, comm.getPersistentDirectory(target));
    }
    this.dataType = dataType;
    MToOneTree gather = new MToOneTree(comm.getChannel(), sources, target,
        finalRcvr, new GatherBatchPartialReceiver(target),
        0, 0, true, MessageTypes.INTEGER, dataType, messageSchema);
    gather.init(comm.getConfig(), dataType, plan, edgeId);
    this.op = gather;
  }

  /**
   * Send a message to be gathered
   *
   * @param source source
   * @param message message
   * @param flags message flag
   * @return true if the message is accepted
   */
  public boolean gather(int source, Object message, int flags) {
    Tuple tuple = new Tuple(source, message);
    return op.send(source, tuple, flags);
  }
}
