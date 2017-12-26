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
package edu.iu.dsc.tws.examples.basic.wordcount;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.core.TWSCommunication;
import edu.iu.dsc.tws.comms.core.TWSNetwork;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.mpi.MPIDataFlowKGather;
import edu.iu.dsc.tws.rsched.spi.container.IContainer;
import edu.iu.dsc.tws.rsched.spi.resource.ResourcePlan;

public class WordCountContainer implements IContainer {
  private MPIDataFlowKGather keyGather;

  private TWSNetwork network;

  private TWSCommunication channel;

  @Override
  public void init(Config cfg, int id, ResourcePlan resourcePlan) {
    TaskPlan taskPlan = WordCountUtils.createWordCountPlan(cfg, resourcePlan, 8);

    network = new TWSNetwork(cfg, taskPlan);
    channel = network.getDataFlowTWSCommunication();

//    keyGather = new MPIDataFlowKGather(channel, )
  }

  private void setupTasks() {
  }
}
