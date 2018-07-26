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

import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.api.ReduceFunction;
import edu.iu.dsc.tws.comms.api.ReduceReceiver;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.dfw.DataFlowReduce;
import edu.iu.dsc.tws.comms.dfw.io.reduce.ReduceStreamingFinalReceiver;
import edu.iu.dsc.tws.comms.dfw.io.reduce.ReduceStreamingPartialReceiver;
import edu.iu.dsc.tws.comms.op.Communicator;

public class SReduce {
  private static final Logger LOG = Logger.getLogger(SReduce.class.getName());

  private DataFlowReduce reduce;

  public SReduce(Communicator comm, TaskPlan plan,
                 Set<Integer> sources, int destination, ReduceFunction fnc,
                 ReduceReceiver rcvr, MessageType dataType) {
    reduce = new DataFlowReduce(comm.getChannel(), sources, destination,
        new ReduceStreamingFinalReceiver(fnc, rcvr),
        new ReduceStreamingPartialReceiver(destination, fnc));
    reduce.init(comm.getConfig(), dataType, plan, comm.nextEdge());
  }

  public boolean reduce(int src, Object message, int flags) {
    return reduce.send(src, message, flags);
  }

  public void progress() {
    reduce.progress();
  }

  public void close() {
    // deregister from the channel
  }
}
