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
import edu.iu.dsc.tws.comms.api.MultiMessageReceiver;
import edu.iu.dsc.tws.comms.core.DataFlowCommunication;
import edu.iu.dsc.tws.comms.core.TaskPlan;

import mpi.MPI;

public class MPIDataFlowCommunication extends DataFlowCommunication {
  private static final Logger LOG = Logger.getLogger(MPIDataFlowCommunication.class.getName());

  private TWSMPIChannel channel;

  @Override
  public void init(Config cfg, TaskPlan taskPlan) {
    super.init(cfg, taskPlan);

    channel = new TWSMPIChannel(cfg, MPI.COMM_WORLD, taskPlan.getThisExecutor());
    LOG.log(Level.INFO, "Initialized MPI dataflow communication");
  }

  @Override
  public void progress() {
    channel.progress();
  }

  public DataFlowOperation reduce(Map<String, Object> properties, MessageType type, int edge,
                                  Set<Integer> sourceTasks, int destTask,
                                  MessageReceiver reduceReceiver, MessageReceiver partialReceiver) {
    // merge with the user specified configuration, user specified will take precedence
    Config mergedCfg = Config.newBuilder().putAll(config).putAll(properties).build();

    // create the dataflow operation
    DataFlowOperation dataFlowOperation = new MPIDataFlowReduce(channel, sourceTasks,
        destTask, reduceReceiver, partialReceiver);

    // intialize the operation
    dataFlowOperation.init(mergedCfg, type, instancePlan, edge);

    return dataFlowOperation;
  }

  public DataFlowOperation broadCast(Map<String, Object> properties, MessageType type, int edge,
                                     int sourceTask, Set<Integer> destTasks,
                                     MessageReceiver receiver) {
    // merge with the user specified configuration, user specified will take precedence
    Config mergedCfg = Config.newBuilder().putAll(config).putAll(properties).build();

    // create the dataflow operation
    DataFlowOperation dataFlowOperation = new MPIDataFlowBroadcast(channel, sourceTask,
        destTasks, receiver);

    // intialize the operation
    dataFlowOperation.init(mergedCfg, type, instancePlan, edge);
    return dataFlowOperation;
  }

  public DataFlowOperation direct(Map<String, Object> properties, MessageType type, int edge,
                                  Set<Integer> sourceTasks, int destTask,
                                  MessageReceiver receiver) {
    // merge with the user specified configuration, user specified will take precedence
    Config mergedCfg = Config.newBuilder().putAll(config).putAll(properties).build();

    // create the dataflow operation
    DataFlowOperation dataFlowOperation = new MPIDirectDataFlowCommunication(channel,
        sourceTasks, destTask, receiver);

    // intialize the operation
    dataFlowOperation.init(mergedCfg, type, instancePlan, edge);
    return dataFlowOperation;
  }

  public DataFlowOperation loadBalance(Map<String, Object> properties, MessageType type, int edge,
                                       Set<Integer> sourceTasks, Set<Integer> destTasks,
                                       MessageReceiver receiver) {
    // merge with the user specified configuration, user specified will take precedence
    Config mergedCfg = Config.newBuilder().putAll(config).putAll(properties).build();

    // create the dataflow operation
    DataFlowOperation dataFlowOperation = new MPIDataFlowLoadBalance(channel,
        sourceTasks, destTasks, receiver);

    // intialize the operation
    dataFlowOperation.init(mergedCfg, type, instancePlan, edge);
    return dataFlowOperation;
  }

  public DataFlowOperation keyedReduce(Map<String, Object> properties, MessageType type,
                                       Set<Integer> edge,
                                       Set<Integer> sourceTasks, Set<Integer> destTasks,
                                       MultiMessageReceiver receiver,
                                       MultiMessageReceiver partial) {
    // merge with the user specified configuration, user specified will take precedence
    Config mergedCfg = Config.newBuilder().putAll(config).putAll(properties).build();

    // create the dataflow operation
    DataFlowOperation dataFlowOperation = new MPIDataFlowMultiReduce(channel,
        sourceTasks, destTasks, receiver, partial, edge);

    // intialize the operation
    dataFlowOperation.init(mergedCfg, type, instancePlan, 0);
    return dataFlowOperation;
  }

  public DataFlowOperation allReduce(Map<String, Object> properties, MessageType type,
                                     int edge1, int edge2,
                                     Set<Integer> sourceTasks, Set<Integer> destTasks,
                                     int middleTask,
                                     MessageReceiver receiver,
                                     MessageReceiver partial) {
    // merge with the user specified configuration, user specified will take precedence
    Config mergedCfg = Config.newBuilder().putAll(config).putAll(properties).build();

    // create the dataflow operation
    DataFlowOperation dataFlowOperation = new MPIDataFlowAllReduce(channel,
        sourceTasks, destTasks, middleTask, receiver, partial, edge1, edge2);

    // intialize the operation
    dataFlowOperation.init(mergedCfg, type, instancePlan, 0);
    return dataFlowOperation;
  }

  public DataFlowOperation gather(Map<String, Object> properties, MessageType type,
                                  int edge1,
                                  Set<Integer> sourceTasks, int destTask,
                                  MessageReceiver receiver) {
    // merge with the user specified configuration, user specified will take precedence
    Config mergedCfg = Config.newBuilder().putAll(config).putAll(properties).build();

    // create the dataflow operation
    DataFlowOperation dataFlowOperation = new MPIDataFlowGather(channel,
        sourceTasks, destTask, receiver, 0, 0, mergedCfg, type, instancePlan, edge1);

    // intialize the operation
    dataFlowOperation.init(mergedCfg, type, instancePlan, edge1);
    return dataFlowOperation;
  }

  @Override
  public DataFlowOperation gather(Map<String, Object> properties, MessageType type,
                                  MessageType keyType, int edge1, Set<Integer> sourceTasks,
                                  int destTask, MessageReceiver receiver) {
    // merge with the user specified configuration, user specified will take precedence
    Config mergedCfg = Config.newBuilder().putAll(config).putAll(properties).build();

    // create the dataflow operation
    DataFlowOperation dataFlowOperation = new MPIDataFlowGather(channel,
        sourceTasks, destTask, receiver, 0, 0, mergedCfg, type, keyType, instancePlan, edge1);

    // intialize the operation
    dataFlowOperation.init(mergedCfg, type, instancePlan, edge1);
    return dataFlowOperation;
  }

  @Override
  public DataFlowOperation gather(Map<String, Object> properties, MessageType type,
                                  MessageType keyType, int edge1, Set<Integer> sourceTasks,
                                  int destTask, MessageReceiver receiver,
                                  MessageReceiver partialRecvr) {
    // merge with the user specified configuration, user specified will take precedence
    Config mergedCfg = Config.newBuilder().putAll(config).putAll(properties).build();

    // create the dataflow operation
    DataFlowOperation dataFlowOperation = new MPIDataFlowGather(channel, sourceTasks, destTask,
        receiver, partialRecvr, 0, 0, mergedCfg, type, keyType, instancePlan, edge1);

    // intialize the operation
    dataFlowOperation.init(mergedCfg, type, instancePlan, edge1);
    return dataFlowOperation;
  }

  @Override
  public DataFlowOperation gather(Map<String, Object> properties, MessageType type,
                                  int edge1, Set<Integer> sourceTasks, int destTask,
                                  MessageReceiver receiver, MessageReceiver partialRecvr) {
    // merge with the user specified configuration, user specified will take precedence
    Config mergedCfg = Config.newBuilder().putAll(config).putAll(properties).build();

    // create the dataflow operation
    DataFlowOperation dataFlowOperation = new MPIDataFlowGather(channel,
        sourceTasks, destTask, receiver, partialRecvr, 0, 0, mergedCfg, type, instancePlan, edge1);

    // intialize the operation
    dataFlowOperation.init(mergedCfg, type, instancePlan, edge1);
    return dataFlowOperation;
  }

  @Override
  public DataFlowOperation keyedGather(Map<String, Object> properties, MessageType type,
                                       Set<Integer> edge,
                                       Set<Integer> sourceTasks, Set<Integer> destTasks,
                                       MultiMessageReceiver receiver) {
    // merge with the user specified configuration, user specified will take precedence
    Config mergedCfg = Config.newBuilder().putAll(config).putAll(properties).build();

    // create the dataflow operation
    DataFlowOperation dataFlowOperation = new MPIDataFlowMultiGather(channel,
        sourceTasks, destTasks, receiver, edge);

    // intialize the operation
    dataFlowOperation.init(mergedCfg, type, instancePlan, 0);
    return dataFlowOperation;
  }

  @Override
  public DataFlowOperation keyedGather(Map<String, Object> properties, MessageType type,
                                       Set<Integer> edge,
                                       Set<Integer> sourceTasks, Set<Integer> destTasks,
                                       MultiMessageReceiver receiver,
                                       MultiMessageReceiver partialRecvr) {
    // merge with the user specified configuration, user specified will take precedence
    Config mergedCfg = Config.newBuilder().putAll(config).putAll(properties).build();

    // create the dataflow operation
    DataFlowOperation dataFlowOperation = new MPIDataFlowMultiGather(channel,
        sourceTasks, destTasks, receiver, partialRecvr, edge);

    // intialize the operation
    dataFlowOperation.init(mergedCfg, type, instancePlan, 0);
    return dataFlowOperation;
  }
}
