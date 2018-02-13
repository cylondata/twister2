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
import edu.iu.dsc.tws.comms.core.DataFlowCommunication;
import edu.iu.dsc.tws.comms.core.NetworkInfo;
import edu.iu.dsc.tws.comms.tcp.net.TCPChannel;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.bootstrap.WorkerInfo;
import edu.iu.dsc.tws.rsched.bootstrap.ZKContext;
import edu.iu.dsc.tws.rsched.bootstrap.ZKController;

/**
 * Create the TCP network using the ZooKeeper
 */
public class TCPNetwork {
  private TCPChannel channel;

  private DataFlowCommunication dataFlowCommunication;

  private ZKController zkController;

  private Config config;

  private JobAPI.Job job;

  private String workerUniqueId;

  public TCPNetwork(Config config, JobAPI.Job j, String wId) {
    this.config = config;
    this.job = j;
    this.workerUniqueId = wId;
  }

  public void initialize() {
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
    NetworkInfo thisNet;
    for (WorkerInfo info : workerInfoList) {
      NetworkInfo networkInfo = new NetworkInfo(info.getWorkerID());
      // todo: we need to have tcp information from worker info
    }

    // lets first start the channel
    channel = new TCPChannel(config, networkInfos, null);
    channel.startFirstPhase();

    // now intialize with zookeeper
    channel.startSecondPhase();
  }
}
