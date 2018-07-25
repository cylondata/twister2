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

import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.api.ReduceFunction;
import edu.iu.dsc.tws.comms.api.ReduceReceiver;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.dfw.DataFlowReduce;
import edu.iu.dsc.tws.comms.dfw.io.reduce.ReduceBatchFinalReceiver;
import edu.iu.dsc.tws.comms.dfw.io.reduce.ReduceBatchPartialReceiver;
import edu.iu.dsc.tws.comms.op.Communicator;

public class BReduce {
  private DataFlowReduce reduce;

  public BReduce(Communicator comm, TaskPlan plan,
                 Set<Integer> sources, int destination, ReduceFunction fnc,
                 ReduceReceiver rcvr, MessageType dataType) {
    reduce = new DataFlowReduce(comm.getChannel(), sources, destination,
        new ReduceBatchPartialReceiver(destination, fnc),
        new ReduceBatchFinalReceiver(fnc, rcvr));
    reduce.init(comm.getConfig(), dataType, plan, comm.nextEdge());
  }

  public boolean reduce(int src, Object message, int flags) {
    return reduce.send(src, message, flags);
  }

  public void finish(int src) {
    reduce.finish(src);
  }

  public void progress() {
    reduce.progress();
  }
}
