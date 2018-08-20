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
package edu.iu.dsc.tws.executor.comm.operations.batch;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.api.TWSChannel;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.op.Communicator;
import edu.iu.dsc.tws.comms.op.batch.BAllGather;
import edu.iu.dsc.tws.executor.api.AbstractParallelOperation;
import edu.iu.dsc.tws.executor.api.EdgeGenerator;
import edu.iu.dsc.tws.task.api.IMessage;

public class AllGatherBatchOperation extends AbstractParallelOperation {
  private static final Logger LOG = Logger.getLogger(AllGatherBatchOperation.class.getName());

  protected BAllGather allGather;
  private Communicator communicator;
  private TaskPlan taskPlan;


  public AllGatherBatchOperation(Config config, TWSChannel network, TaskPlan tPlan) {
    super(config, network, tPlan);
    this.communicator = new Communicator(config, network);
    this.taskPlan = tPlan;
  }

  public void prepare(Set<Integer> sources, Set<Integer> dest, EdgeGenerator e,
                      MessageType dataType, String edgeName) {
    this.edge = e;
    this.allGather = new BAllGather(communicator, taskPlan, sources, dest, new AllGatherReceiver(),
        dataType);
  }

  @Override
  public boolean send(int source, IMessage message, int flags) {
    return allGather.reduce(source, message.getContent(), flags);
  }

  // TODO : Send with dest must be implemented
  @Override
  public void send(int source, IMessage message, int dest, int flags) {
    throw new RuntimeException("Send with dest not Implemented in AllGatherBatchOps");
  }

  @Override
  public boolean progress() {
    return allGather.progress();
  }


  //TODO : AllGather comms hasPending must be implemented.

  public boolean hasPending() {
    return true;
  }


  private class AllGatherReceiver implements MessageReceiver {

    @Override
    public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {

    }

    @Override
    public boolean onMessage(int source, int destination, int target, int flags, Object object) {
      return false;
    }

    @Override
    public void onFinish(int source) {

    }

    @Override
    public boolean progress() {
      return false;
    }
  }

}
