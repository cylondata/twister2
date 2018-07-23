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
import edu.iu.dsc.tws.comms.dfw.io.reduce.ReduceBatchFinalReceiver;
import edu.iu.dsc.tws.comms.dfw.io.reduce.ReduceBatchPartialReceiver;
import edu.iu.dsc.tws.comms.op.Communicator;
import edu.iu.dsc.tws.comms.op.OperationSemantics;

public class SReduce {
  private static final Logger LOG = Logger.getLogger(SReduce.class.getName());

  private DataFlowReduce reduce;

  public SReduce(Communicator comm, OperationSemantics semantics, TaskPlan plan,
                 Set<Integer> sources, int destination, ReduceFunction fnc,
                 ReduceReceiver rcvr, MessageType dataType) {
    if (semantics == OperationSemantics.BATCH) {
      reduce = new DataFlowReduce(comm.getChannel(), sources, destination,
          new ReduceBatchPartialReceiver(destination, fnc),
          new ReduceBatchFinalReceiver(fnc, rcvr));
      reduce.init(comm.getConfig(), dataType, plan, comm.nextEdge());
    } else if (semantics == OperationSemantics.STREAMING) {
      reduce = new DataFlowReduce(comm.getChannel(), sources, destination,
          new ReduceBatchPartialReceiver(destination, fnc),
          new ReduceBatchFinalReceiver(fnc, rcvr));
      reduce.init(comm.getConfig(), dataType, plan, comm.nextEdge());
    } else {
      throw new UnsupportedOperationException("Not supported semantics: " + semantics);
    }
  }

  public boolean send(int src, Object message, int flags) {
    return reduce.send(src, message, flags);
  }
}
