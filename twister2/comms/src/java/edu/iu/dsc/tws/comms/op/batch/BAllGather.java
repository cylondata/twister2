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

public class BAllGather {
  private DataFlowAllGather gather;

  public BAllGather(Communicator comm, TaskPlan plan,
                    Set<Integer> sources, Set<Integer> destination,
                    BulkReceiver rcvr, MessageType dataType) {
    if (sources.size() == 0) {
      throw new IllegalArgumentException("The sources cannot be empty");
    }

    if (destination.size() == 0) {
      throw new IllegalArgumentException("The destination cannot be empty");
    }
    int middleTask = comm.nextId();

    int firstSource = sources.iterator().next();
    plan.addChannelToExecutor(plan.getExecutorForChannel(firstSource), middleTask);

    gather = new DataFlowAllGather(comm.getChannel(), sources, destination, middleTask, rcvr,
        comm.nextEdge(), comm.nextEdge(), false);
    gather.init(comm.getConfig(), dataType, plan, comm.nextEdge());
  }

  public boolean reduce(int src, Object message, int flags) {
    return gather.send(src, message, flags);
  }

  public boolean progress() {
    return gather.progress();
  }

  public boolean hasPending() {
    return !gather.isComplete();
  }

  public void finish(int source) {
    gather.finish(source);
  }
}
