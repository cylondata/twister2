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
package edu.iu.dsc.tws.comms.mpi;

import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.Operation;
import edu.iu.dsc.tws.comms.core.DataFlowCommunication;
import edu.iu.dsc.tws.comms.core.TaskPlan;

import mpi.MPI;

public class MPIDataFlowCommunication extends DataFlowCommunication {
  private static final Logger LOG = Logger.getLogger(MPIDataFlowCommunication.class.getName());

  private TWSMPIChannel channel;

  @Override
  public void init(Config cfg, TaskPlan taskPlan) {
    super.init(cfg, taskPlan);

    channel = new TWSMPIChannel(cfg, MPI.COMM_WORLD);
  }

  @Override
  public DataFlowOperation create(Operation operation) {
    if (operation == Operation.BROADCAST) {
      return new MPIDataFlowBroadcast(channel);
    } else if (operation == Operation.REDUCE) {
      return new MPIDataFlowReduce(channel);
    } else if (operation == Operation.ALLGATHER) {
      return null;
    } else if (operation == Operation.LOADBALANCE) {
      return new MPILoadBalance(channel);
    } else if (operation == Operation.PARTITION) {
      return null;
    }
    return null;
  }

  @Override
  public void progress() {
    channel.progress();
  }
}
