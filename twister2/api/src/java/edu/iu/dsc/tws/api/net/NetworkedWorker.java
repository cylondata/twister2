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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.discovery.IWorkerController;
import edu.iu.dsc.tws.common.discovery.WorkerNetworkInfo;
import edu.iu.dsc.tws.common.net.NetworkInfo;
import edu.iu.dsc.tws.common.net.tcp.TCPChannel;
import edu.iu.dsc.tws.common.net.tcp.TCPContext;
import edu.iu.dsc.tws.comms.api.TWSChannel;
import edu.iu.dsc.tws.comms.tcp.TWSTCPChannel;
import edu.iu.dsc.tws.rsched.spi.container.IPersistentVolume;
import edu.iu.dsc.tws.rsched.spi.container.IVolatileVolume;
import edu.iu.dsc.tws.rsched.spi.container.IWorker;
import edu.iu.dsc.tws.rsched.spi.resource.ResourcePlan;

public class NetworkedWorker implements IWorker {
  private static final Logger LOG = Logger.getLogger(NetworkedWorker.class.getName());

  @Override
  public void init(Config config, int id, ResourcePlan resourcePlan,
                   IWorkerController workerController, IPersistentVolume persistentVolume,
                   IVolatileVolume volatileVolume) {
    TWSChannel channel = initializeNetwork(config, workerController);
    // now lets call the initial of the actual worker
  }

  public static TWSChannel initializeNetwork(Config config, IWorkerController wController) {
    TCPChannel channel;
    int index = wController.getWorkerNetworkInfo().getWorkerID();
    Integer workerPort = wController.getWorkerNetworkInfo().getWorkerPort();
    String localIp = wController.getWorkerNetworkInfo().getWorkerIP().getHostName();
    try {
      channel = initNetworkServer(config,
          new WorkerNetworkInfo(InetAddress.getByName(localIp), workerPort, index), index);
    } catch (UnknownHostException e) {
      throw new RuntimeException("Failed to get network address: " + localIp, e);
    }

    // now start the client connections
    List<WorkerNetworkInfo> wInfo = wController.getWorkerList();
    List<NetworkInfo> nInfos = new ArrayList<>();
    for (WorkerNetworkInfo w : wInfo) {
      NetworkInfo networkInfo = new NetworkInfo(w.getWorkerID());
      networkInfo.addProperty(TCPContext.NETWORK_PORT, w.getWorkerPort());
      networkInfo.addProperty(TCPContext.NETWORK_HOSTNAME, w.getWorkerIP().toString());
      nInfos.add(networkInfo);
    }
    // start the connections
    channel.startConnections(nInfos, null);
    // now lets wait for connections to be established
    channel.waitForConnections();

    // now lets create a tcp channel
    return new TWSTCPChannel(config, index, channel);
  }

  /**
   * Start the TCP servers here
   * @param cfg the configuration
   * @param networkInfo network info
   * @param workerId worker id
   */
  private static TCPChannel initNetworkServer(Config cfg, WorkerNetworkInfo networkInfo,
                                              int workerId) {
    NetworkInfo netInfo = new NetworkInfo(workerId);
    netInfo.addProperty(TCPContext.NETWORK_HOSTNAME, networkInfo.getWorkerIP().getHostName());
    netInfo.addProperty(TCPContext.NETWORK_PORT, networkInfo.getWorkerPort());
    TCPChannel channel = new TCPChannel(cfg, netInfo);
    channel.startListening();
    return channel;
  }
}
