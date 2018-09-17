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
package edu.iu.dsc.tws.comms.op.batch;

import java.util.Set;

import edu.iu.dsc.tws.comms.api.BulkReceiver;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.dfw.DataFlowAllGather;
import edu.iu.dsc.tws.comms.op.Communicator;

/**
 * Batch ALLGather Operation
 */
public class BAllGather {
  /**
   * The actual operation
   */
  private DataFlowAllGather gather;

  /**
   * Construct a AllGather operation
   * @param comm the communicator
   * @param plan task plan
   * @param sources source tasks
   * @param targets target tasks
   * @param rcvr receiver
   * @param dataType data type
   */
  public BAllGather(Communicator comm, TaskPlan plan,
                    Set<Integer> sources, Set<Integer> targets,
                    BulkReceiver rcvr, MessageType dataType) {
    if (sources.size() == 0) {
      throw new IllegalArgumentException("The sources cannot be empty");
    }

    if (targets.size() == 0) {
      throw new IllegalArgumentException("The destination cannot be empty");
    }
    int middleTask = comm.nextId();

    int firstSource = sources.iterator().next();
    plan.addChannelToExecutor(plan.getExecutorForChannel(firstSource), middleTask);

    gather = new DataFlowAllGather(comm.getChannel(), sources, targets, middleTask, rcvr,
        comm.nextEdge(), comm.nextEdge(), false);
    gather.init(comm.getConfig(), dataType, plan, comm.nextEdge());
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
    return gather.send(src, message, flags);
  }

  /**
   * Progress the gather operation, if not called, messages will not be processed
   *
   * @return true if further progress is needed
   */
  public boolean progress() {
    return gather.progress();
  }

  /**
   * Weather we have messages pending
   * @return true if there are messages pending
   */
  public boolean hasPending() {
    return !gather.isComplete();
  }

  /**
   * Indicate the end of the communication
   * @param source the source that is ending
   */
  public void finish(int source) {
    gather.finish(source);
  }
}
