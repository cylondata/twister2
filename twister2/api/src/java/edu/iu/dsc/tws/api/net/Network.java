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
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.controller.IWorkerController;
import edu.iu.dsc.tws.common.exceptions.TimeoutException;
import edu.iu.dsc.tws.common.net.NetworkInfo;
import edu.iu.dsc.tws.common.net.tcp.TCPChannel;
import edu.iu.dsc.tws.common.net.tcp.TCPContext;
import edu.iu.dsc.tws.comms.api.TWSChannel;
import edu.iu.dsc.tws.comms.mpi.TWSMPIChannel;
import edu.iu.dsc.tws.comms.tcp.TWSTCPChannel;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;

import mpi.MPI;

public final class Network {
  public static final Logger LOG = Logger.getLogger(Network.class.getName());

  private Network() {
  }

  public static TWSChannel initializeChannel(Config config, IWorkerController wController) {
    if (config.getStringValue("twister2.network.channel.class").equals(
        "edu.iu.dsc.tws.comms.dfw.tcp.TWSTCPChannel")) {
      return initializeTCPNetwork(config, wController);
    } else {
      return initializeMPIChannel(config, wController);
    }
  }

  private static TWSChannel initializeMPIChannel(Config config,
                                                 IWorkerController wController) {
    //first get the communication config file
    return new TWSMPIChannel(config, MPI.COMM_WORLD, wController.getWorkerInfo().getWorkerID());
  }

  private static TWSChannel initializeTCPNetwork(Config config,
                                                 IWorkerController wController) {
    TCPChannel channel;
    int index = wController.getWorkerInfo().getWorkerID();
    Integer workerPort = wController.getWorkerInfo().getPort();
    String localIp = wController.getWorkerInfo().getWorkerIP();

    channel = createChannel(config, localIp, workerPort, index);
    // now lets start listening before sending the ports to master,
    channel.startListening();

    // wait for everyone to start the job master
    try {
      wController.waitOnBarrier();
    } catch (TimeoutException timeoutException) {
      LOG.log(Level.SEVERE, timeoutException.getMessage(), timeoutException);
      return null;
    }

    // now talk to a central server and get the information about the worker
    // this is a synchronization step
    List<JobMasterAPI.WorkerInfo> wInfo = wController.getJoinedWorkers();

    // lets start the client connections now
    List<NetworkInfo> nInfos = new ArrayList<>();
    for (JobMasterAPI.WorkerInfo w : wInfo) {
      NetworkInfo networkInfo = new NetworkInfo(w.getWorkerID());
      networkInfo.addProperty(TCPContext.NETWORK_PORT, w.getPort());
      networkInfo.addProperty(TCPContext.NETWORK_HOSTNAME, w.getWorkerIP());
      nInfos.add(networkInfo);
    }

    // start the connections
    channel.startConnections(nInfos);
    // now lets wait for connections to be established
    channel.waitForConnections();

    // now lets create a tcp channel
    return new TWSTCPChannel(config, wController.getWorkerInfo().getWorkerID(), channel);
  }

  /**
   * Start the TCP servers here
   * @param cfg the configuration
   * @param workerId worker id
   */
  private static TCPChannel createChannel(Config cfg, String workerIP, int workerPort,
                                          int workerId) {
    NetworkInfo netInfo = new NetworkInfo(workerId);
    netInfo.addProperty(TCPContext.NETWORK_HOSTNAME, workerIP);
    netInfo.addProperty(TCPContext.NETWORK_PORT, workerPort);
    return new TCPChannel(cfg, netInfo);
  }
}
