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
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.comms.Communicator;
import edu.iu.dsc.tws.api.comms.LogicalPlan;
import edu.iu.dsc.tws.api.comms.ReduceFunction;
import edu.iu.dsc.tws.api.comms.SingularReceiver;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageType;
import edu.iu.dsc.tws.comms.dfw.BaseOperation;
import edu.iu.dsc.tws.comms.dfw.MToOneTree;
import edu.iu.dsc.tws.comms.dfw.io.reduce.ReduceStreamingFinalReceiver;
import edu.iu.dsc.tws.comms.dfw.io.reduce.ReduceStreamingPartialReceiver;

/**
 * Streaming Reduce Operation
 */
public class SReduce extends BaseOperation {
  private static final Logger LOG = Logger.getLogger(SReduce.class.getName());

  /**
   * Construct a Streaming Reduce operation
   *
   * @param comm the communicator
   * @param plan task plan
   * @param sources source tasks
   * @param target target tasks
   * @param fnc reduce function
   * @param rcvr receiver
   * @param dataType data type
   */
  public SReduce(Communicator comm, LogicalPlan plan,
                 Set<Integer> sources, int target,
                 MessageType dataType, ReduceFunction fnc, SingularReceiver rcvr, int edgeId) {
    super(comm.getChannel());
    MToOneTree reduce = new MToOneTree(comm.getChannel(), sources, target,
        new ReduceStreamingFinalReceiver(fnc, rcvr),
        new ReduceStreamingPartialReceiver(target, fnc));
    reduce.init(comm.getConfig(), dataType, plan, edgeId);
    op = reduce;
  }

  public SReduce(Communicator comm, LogicalPlan plan,
                 Set<Integer> sources, int target,
                 MessageType dataType, ReduceFunction fnc, SingularReceiver rcvr) {
    this(comm, plan, sources, target, dataType, fnc, rcvr, comm.nextEdge());
  }

  /**
   * Send a message to be reduced
   *
   * @param src source
   * @param message message
   * @param flags message flag
   * @return true if the message is accepted
   */
  public boolean reduce(int src, Object message, int flags) {
    return op.send(src, message, flags);
  }
}
