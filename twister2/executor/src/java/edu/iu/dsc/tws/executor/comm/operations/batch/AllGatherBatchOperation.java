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

import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.dfw.DataFlowAllGather;
import edu.iu.dsc.tws.comms.op.Communicator;
import edu.iu.dsc.tws.executor.api.AbstractParallelOperation;
import edu.iu.dsc.tws.task.api.IMessage;

public class AllGatherBatchOperation extends AbstractParallelOperation {
  private static final Logger LOG = Logger.getLogger(AllGatherBatchOperation.class.getName());

  protected DataFlowAllGather op;


  public AllGatherBatchOperation(Config config, Communicator network, TaskPlan tPlan) {
    super(config, network, tPlan);
  }

  @Override
  public boolean send(int source, IMessage message, int flags) {
    return op.send(source, message.getContent(), flags);
  }

  @Override
  public boolean progress() {
    return op.progress() && hasPending();
  }

  public boolean hasPending() {
    return !op.isComplete();
  }
}
