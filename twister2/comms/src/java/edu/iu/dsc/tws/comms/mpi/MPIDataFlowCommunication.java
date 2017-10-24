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

import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.api.MessageType;
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
    LOG.log(Level.INFO, "Initialized MPI dataflow communication");
  }

  @Override
  public void progress() {
    channel.progress();
  }

  public DataFlowOperation reduce(Map<String, Object> properties, MessageType type, int edge,
                                  Set<Integer> sourceTasks, int destTask,
                                  MessageReceiver reduceReceiver, MessageReceiver partialReceiver) {
    LOG.info("Merging configurations");
    // merge with the user specified configuration, user specified will take precedence
    Config mergedCfg = Config.newBuilder().putAll(config).putAll(properties).build();
    LOG.info("Merged configurations");

    // create the dataflow operation
    DataFlowOperation dataFlowOperation = new MPIDataFlowReduce(channel, sourceTasks, destTask);
    LOG.info("Created dataflow operation");

    // intialize the operation
    dataFlowOperation.init(mergedCfg, type, instancePlan, edge, reduceReceiver, partialReceiver);
    LOG.info("Intiailize dataflow operation");

    return dataFlowOperation;
  }

  public DataFlowOperation broadCast(Map<String, Object> properties, MessageType type, int edge,
                                     int sourceTask, Set<Integer> destTasks,
                                     MessageReceiver receiver) {
    LOG.info("Merging configurations");
    // merge with the user specified configuration, user specified will take precedence
    Config mergedCfg = Config.newBuilder().putAll(config).putAll(properties).build();
    LOG.info("Merged configurations");

    // create the dataflow operation
    DataFlowOperation dataFlowOperation = new MPIDataFlowBroadcast(channel, sourceTask, destTasks);
    LOG.info("Created dataflow operation");

    // intialize the operation
    dataFlowOperation.init(mergedCfg, type, instancePlan, edge, receiver, null);
    LOG.info("Intiailize dataflow operation");
    return dataFlowOperation;
  }

  public DataFlowOperation direct(Map<String, Object> properties, MessageType type, int edge,
                                  Set<Integer> sourceTasks, int destTask,
                                  MessageReceiver receiver) {
    LOG.info("Merging configurations");
    // merge with the user specified configuration, user specified will take precedence
    Config mergedCfg = Config.newBuilder().putAll(config).putAll(properties).build();
    LOG.info("Merged configurations");

    // create the dataflow operation
    DataFlowOperation dataFlowOperation = new MPIDirectDataFlowCommunication(channel,
        sourceTasks, destTask);
    LOG.info("Created dataflow operation");

    // intialize the operation
    dataFlowOperation.init(mergedCfg, type, instancePlan, edge, receiver, null);
    LOG.info("Intiailize dataflow operation");
    return dataFlowOperation;
  }
}
