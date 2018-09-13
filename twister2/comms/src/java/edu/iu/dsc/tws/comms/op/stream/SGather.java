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
import java.util.logging.Logger;

import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.dfw.DataFlowGather;
import edu.iu.dsc.tws.comms.dfw.io.gather.GatherStreamingFinalReceiver;
import edu.iu.dsc.tws.comms.dfw.io.gather.GatherStreamingPartialReceiver;
import edu.iu.dsc.tws.comms.op.Communicator;

public class SGather {
  private static final Logger LOG = Logger.getLogger(SReduce.class.getName());

  private DataFlowGather gather;

  public SGather(Communicator comm, TaskPlan plan,
                 Set<Integer> sources, int destination,
                 MessageReceiver rcvr, MessageType dataType) {
    gather = new DataFlowGather(comm.getChannel(), sources, destination,
        new GatherStreamingFinalReceiver(rcvr),
        new GatherStreamingPartialReceiver(), 0, 0,
        comm.getConfig(), dataType, plan, comm.nextEdge());
    gather.init(comm.getConfig(), dataType, plan, comm.nextEdge());
  }

  public boolean gather(int src, Object message, int flags) {
    return gather.send(src, message, flags);
  }

  public boolean hasPending() {
    return !gather.isComplete();
  }

  public boolean progress() {
    return gather.progress();
  }

  public void close() {
    // deregister from the channel
  }

  public void finish(int src) {
    gather.finish(src);
  }
}
