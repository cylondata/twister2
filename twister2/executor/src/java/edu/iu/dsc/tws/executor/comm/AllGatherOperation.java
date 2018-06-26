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
package edu.iu.dsc.tws.executor.comm;

import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.TWSChannel;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.dfw.DataFlowAllGather;
import edu.iu.dsc.tws.task.api.IMessage;

public class AllGatherOperation extends AbstractParallelOperation {
  private static final Logger LOG = Logger.getLogger(AllGatherOperation.class.getName());

  protected DataFlowAllGather op;


  public AllGatherOperation(Config config, TWSChannel network, TaskPlan tPlan) {
    super(config, network, tPlan);
  }

  @Override
  public void send(int source, IMessage message) {
    op.send(source, message.getContent(), 0);
  }

  @Override
  public void send(int source, IMessage message, int dest) {
    op.send(source, message.getContent(), 0, dest);
  }

  @Override
  public void progress() {
    op.progress();
  }
}
