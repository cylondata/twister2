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
package edu.iu.dsc.tws.api.net;

import java.util.ArrayList;
import java.util.List;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.core.NetworkInfo;
import edu.iu.dsc.tws.comms.core.TWSCommunication;
import edu.iu.dsc.tws.comms.core.TaskPlan;
import edu.iu.dsc.tws.comms.mpi.MPIDataFlowCommunication;
import edu.iu.dsc.tws.comms.tcp.TWSTCPChannel;
import edu.iu.dsc.tws.comms.tcp.net.TCPChannel;
import edu.iu.dsc.tws.comms.tcp.net.TCPContext;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.bootstrap.WorkerInfo;
import edu.iu.dsc.tws.rsched.bootstrap.ZKContext;
import edu.iu.dsc.tws.rsched.bootstrap.ZKController;

/**
 * Create the TCP network using the ZooKeeper
 */
public class TCPNetwork {
  private TCPChannel channel;

  private TWSCommunication dataFlowCommunication;

  private ZKController zkController;

  private Config config;

  private JobAPI.Job job;

  private String workerUniqueId;

  private TaskPlan taskPlan;

  public TCPNetwork(Config config, JobAPI.Job j, String wId, TaskPlan plan) {
    this.config = config;
    this.job = j;
    this.workerUniqueId = wId;
    this.taskPlan = plan;
  }

  public void initialize() {
    NetworkInfo networkInfo = new NetworkInfo(-1);
    // we need to first start the server part of tcp
    // lets first start the channel
    channel = new TCPChannel(config, networkInfo);
    channel.startFirstPhase();

    // first lets intialize the zk
    zkController = new ZKController(config, job.getJobName(), workerUniqueId);
    zkController.initialize();

    // the amount of time to wait for all workers to join a job
    int timeLimit =  ZKContext.maxWaitTimeForAllWorkersToJoin(config);

    List<WorkerInfo> workerInfoList = zkController.waitForAllWorkersToJoin(
        job.getJobResources().getNoOfContainers(), timeLimit);

    if (workerInfoList == null) {
      throw new RuntimeException("Error getting the worker list from ZooKeeper");
    }

    List<NetworkInfo> networkInfos = new ArrayList<>();
    NetworkInfo thisNet = null;
    for (WorkerInfo info : workerInfoList) {
      NetworkInfo netInfo = new NetworkInfo(info.getWorkerID());
      netInfo.addProperty(TCPContext.NETWORK_HOSTNAME, info.getWorkerIP());
      netInfo.addProperty(TCPContext.NETWORK_PORT, info.getWorkerPort());
      // todo: we need to have tcp information from worker info
      if (workerUniqueId.equals(info.getWorkerName())) {
        thisNet = netInfo;
      }
      networkInfos.add(netInfo);
    }
    // now intialize with zookeeper
    channel.startSecondPhase(networkInfos, thisNet);

    TWSTCPChannel twstcpChannel = new TWSTCPChannel(config, taskPlan.getThisExecutor(), channel);
    // now lets create the dataflow communication
    dataFlowCommunication = new MPIDataFlowCommunication();
    dataFlowCommunication.init(config, taskPlan, twstcpChannel);
  }

  public TWSCommunication getDataFlowCommunication() {
    return dataFlowCommunication;
  }
}
