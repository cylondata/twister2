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

import java.util.Comparator;
import java.util.Set;

import edu.iu.dsc.tws.comms.api.BulkReceiver;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.dfw.DataFlowGather;
import edu.iu.dsc.tws.comms.dfw.io.gather.GatherBatchFinalReceiver;
import edu.iu.dsc.tws.comms.dfw.io.gather.GatherBatchPartialReceiver;
import edu.iu.dsc.tws.comms.op.Communicator;

public class BGather {
  private DataFlowGather gather;

  public BGather(Communicator comm, TaskPlan plan,
                 Set<Integer> sources, int destinations,
                 MessageType dataType,
                 BulkReceiver rcvr) {
    this.gather = new DataFlowGather(comm.getChannel(), sources, destinations,
        new GatherBatchFinalReceiver(rcvr), new GatherBatchPartialReceiver(destinations),
        0, 0, comm.getConfig(), dataType, plan, comm.nextEdge());
    this.gather.init(comm.getConfig(), dataType, plan, comm.nextEdge());
  }

  public BGather(Communicator comm, TaskPlan plan,
                 Set<Integer> sources, int destinations,
                 MessageType dataType,
                 BulkReceiver rcvr, Comparator<Object> comparator) {
    this.gather = new DataFlowGather(comm.getChannel(), sources, destinations,
        new GatherBatchFinalReceiver(rcvr), new GatherBatchPartialReceiver(destinations),
        0, 0, comm.getConfig(), dataType, plan, comm.nextEdge());
    this.gather.init(comm.getConfig(), dataType, plan, comm.nextEdge());
  }

  public boolean gather(int source, Object message, int flags) {
    return gather.send(source, message, flags);
  }

  public boolean hasPending() {
    return !gather.isComplete();
  }

  public void finish(int source) {
    gather.finish(source);
  }

  public boolean progress() {
    return gather.progress();
  }
}
