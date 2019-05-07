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
package edu.iu.dsc.tws.comms.api.batch;

import java.util.Set;

import edu.iu.dsc.tws.comms.api.Communicator;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.api.ReduceFunction;
import edu.iu.dsc.tws.comms.api.SingularReceiver;
import edu.iu.dsc.tws.comms.api.TaskPlan;
import edu.iu.dsc.tws.comms.dfw.AllReduce;

/**
 * Batch ALLReduce Operation
 */
public class BAllReduce {
  /**
   * The actual operation
   */
  private AllReduce reduce;

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
  public BAllReduce(Communicator comm, TaskPlan plan,
                    Set<Integer> sources, Set<Integer> targets, ReduceFunction fnc,
                    SingularReceiver rcvr, MessageType dataType) {
    if (sources.size() == 0) {
      throw new IllegalArgumentException("The sources cannot be empty");
    }

    if (targets.size() == 0) {
      throw new IllegalArgumentException("The destination cannot be empty");
    }

    int middleTask = comm.nextId();
    int firstSource = sources.iterator().next();
    plan.addChannelToExecutor(plan.getExecutorForChannel(firstSource), middleTask);

    reduce = new AllReduce(comm.getConfig(), comm.getChannel(), plan, sources,
        targets, middleTask, fnc, rcvr, dataType, comm.nextEdge(), comm.nextEdge(), false);
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
    return reduce.send(src, message, flags);
  }

  /**
   * Progress the operation, if not called, messages will not be processed
   *
   * @return true if further progress is needed
   */
  public boolean progress() {
    return reduce.progress();
  }

  /**
   * Weather we have messages pending
   * @return true if there are messages pending
   */
  public boolean hasPending() {
    return !reduce.isComplete();
  }

  /**
   * Indicate the end of the communication
   * @param source the source that is ending
   */
  public void finish(int source) {
    reduce.finish(source);
  }

  public void close() {
    // deregister from the channel
    reduce.close();
  }

  /**
   * Clean the operation, this doesn't close it
   */
  public void refresh() {
    reduce.clean();
  }
}
