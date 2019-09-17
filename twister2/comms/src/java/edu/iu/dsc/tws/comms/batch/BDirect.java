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

import java.util.ArrayList;
import java.util.List;

import edu.iu.dsc.tws.api.comms.BulkReceiver;
import edu.iu.dsc.tws.api.comms.CommunicationContext;
import edu.iu.dsc.tws.api.comms.Communicator;
import edu.iu.dsc.tws.api.comms.LogicalPlan;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageType;
import edu.iu.dsc.tws.api.comms.packing.MessageSchema;
import edu.iu.dsc.tws.comms.dfw.BaseOperation;
import edu.iu.dsc.tws.comms.dfw.OneToOne;
import edu.iu.dsc.tws.comms.dfw.io.direct.DirectBatchFinalReceiver;
import edu.iu.dsc.tws.comms.utils.LogicalPlanBuilder;

public class BDirect extends BaseOperation {
  public BDirect(Communicator comm, LogicalPlan plan,
                 List<Integer> sources, List<Integer> targets,
                 BulkReceiver rcvr, MessageType dataType, int edgeId,
                 MessageSchema messageSchema) {
    super(comm, false, CommunicationContext.DIRECT);
    if (sources.size() == 0) {
      throw new IllegalArgumentException("The sources cannot be empty");
    }

    if (targets.size() == 0) {
      throw new IllegalArgumentException("The destination cannot be empty");
    }

    int middleTask = comm.nextId();
    int firstSource = sources.iterator().next();
    plan.addLogicalIdToWorker(plan.getWorkerForForLogicalId(firstSource), middleTask);

    op = new OneToOne(comm.getChannel(), sources, targets,
        new DirectBatchFinalReceiver(rcvr), comm.getConfig(), dataType, plan,
        edgeId, messageSchema);
  }

  /**
   * Construct a Batch AllReduce operation
   *
   * @param comm the communicator
   * @param plan task plan
   * @param sources source tasks
   * @param targets target tasks
   * @param rcvr receiver
   * @param dataType data type
   */
  public BDirect(Communicator comm, LogicalPlan plan,
                 List<Integer> sources, List<Integer> targets,
                 BulkReceiver rcvr, MessageType dataType) {
    this(comm, plan, sources, targets, rcvr, dataType, comm.nextEdge(), MessageSchema.noSchema());
  }

  public BDirect(Communicator comm, LogicalPlanBuilder logicalPlanBuilder,
                 BulkReceiver rcvr, MessageType dataType) {
    this(comm, logicalPlanBuilder.build(),
        new ArrayList<>(logicalPlanBuilder.getSources()),
        new ArrayList<>(logicalPlanBuilder.getTargets()),
        rcvr, dataType, comm.nextEdge(), MessageSchema.noSchema());
  }

  public BDirect(Communicator comm, LogicalPlan plan,
                 List<Integer> sources, List<Integer> targets,
                 BulkReceiver rcvr, MessageType dataType, MessageSchema messageSchema) {
    this(comm, plan, sources, targets, rcvr, dataType, comm.nextEdge(), messageSchema);
  }

  /**
   * Send a message to be reduced
   *
   * @param src source
   * @param message message
   * @param flags message flag
   * @return true if the message is accepted
   */
  public boolean direct(int src, Object message, int flags) {
    return op.send(src, message, flags);
  }
}
