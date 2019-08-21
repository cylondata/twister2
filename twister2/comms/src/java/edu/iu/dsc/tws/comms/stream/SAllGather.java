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

import edu.iu.dsc.tws.api.comms.BulkReceiver;
import edu.iu.dsc.tws.api.comms.CommunicationContext;
import edu.iu.dsc.tws.api.comms.Communicator;
import edu.iu.dsc.tws.api.comms.LogicalPlan;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageType;
import edu.iu.dsc.tws.api.comms.packing.MessageSchema;
import edu.iu.dsc.tws.comms.dfw.AllGather;
import edu.iu.dsc.tws.comms.dfw.BaseOperation;
import edu.iu.dsc.tws.comms.utils.LogicalPlanBuilder;

/**
 * Streaming ALLGather Operation
 */
public class SAllGather extends BaseOperation {
  /**
   * Construct a Streaming AllGather operation
   *
   * @param comm the communicator
   * @param plan task plan
   * @param sources source tasks
   * @param targets target tasks
   * @param rcvr receiver
   * @param dataType data type
   */
  public SAllGather(Communicator comm, LogicalPlan plan,
                    Set<Integer> sources, Set<Integer> targets,
                    BulkReceiver rcvr, MessageType dataType, int gtrEdgeId,
                    int bcstEdgeId, MessageSchema messageSchema) {
    super(comm, true, CommunicationContext.ALLGATHER);
    if (sources.size() == 0) {
      throw new IllegalArgumentException("The sources cannot be empty");
    }

    if (targets.size() == 0) {
      throw new IllegalArgumentException("The destination cannot be empty");
    }
    int middleTask = comm.nextId();

    int firstSource = sources.iterator().next();
    plan.addChannelToExecutor(plan.getExecutorForChannel(firstSource), middleTask);
    op = new AllGather(comm.getConfig(), comm.getChannel(), plan, sources, targets,
        middleTask, rcvr, dataType, gtrEdgeId, bcstEdgeId, true, messageSchema);
  }

  public SAllGather(Communicator comm, LogicalPlan plan,
                    Set<Integer> sources, Set<Integer> targets,
                    BulkReceiver rcvr, MessageType dataType) {
    this(comm, plan, sources, targets, rcvr, dataType, comm.nextEdge(),
        comm.nextEdge(), MessageSchema.noSchema());
  }

  public SAllGather(Communicator comm, LogicalPlanBuilder logicalPlanBuilder,
                    BulkReceiver rcvr, MessageType dataType) {
    this(comm, logicalPlanBuilder.build(), logicalPlanBuilder.getSources(),
        logicalPlanBuilder.getTargets(), rcvr, dataType, comm.nextEdge(),
        comm.nextEdge(), MessageSchema.noSchema());
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
    return op.send(src, message, flags);
  }
}
