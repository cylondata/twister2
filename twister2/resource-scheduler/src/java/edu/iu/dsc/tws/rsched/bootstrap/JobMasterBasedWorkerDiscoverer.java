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
package edu.iu.dsc.tws.rsched.bootstrap;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.discovery.IWorkerDiscoverer;
import edu.iu.dsc.tws.common.discovery.WorkerNetworkInfo;
import edu.iu.dsc.tws.master.client.JobMasterClient;
import edu.iu.dsc.tws.master.client.WorkerDiscoverer;

/**
 * Job master based worker controller. This one talks to job
 * master to discover the workers
 */
public class JobMasterBasedWorkerDiscoverer implements IWorkerDiscoverer {
  private static final Logger LOG = Logger.getLogger(
      JobMasterBasedWorkerDiscoverer.class.getName());

  private WorkerNetworkInfo networkInfo;

  private JobMasterClient masterClient;

  private int numberOfWorkers;

  public JobMasterBasedWorkerDiscoverer(Config cfg, int workerId, int numberOfWorkers,
                                        String jobMasterHost, int jobMasterPort,
                                        Map<String, Integer> nameToPorts,
                                        Map<String, String> nameToHost) {
    this.numberOfWorkers = numberOfWorkers;
    int port = nameToPorts.get("worker");
    String host = nameToHost.get("worker");
    try {
      this.networkInfo = new WorkerNetworkInfo(InetAddress.getByName(host), port, workerId);
    } catch (UnknownHostException e) {
      throw new RuntimeException("Failed to resolve hostname of master: " + host, e);
    }

    this.masterClient = createMasterClient(cfg, jobMasterPort, jobMasterHost,
        networkInfo, numberOfWorkers);
  }

  @Override
  public WorkerNetworkInfo getWorkerNetworkInfo() {
    return networkInfo;
  }

  /**
   * Create the job master client to get information about the workers
   */
  private static JobMasterClient createMasterClient(Config cfg, int masterPort, String masterHost,
                                                    WorkerNetworkInfo networkInfo,
                                                    int numberContainers) {
    // we start the job master client
    JobMasterClient jobMasterClient = new JobMasterClient(cfg,
        networkInfo, masterHost, masterPort, numberContainers);
    LOG.log(Level.INFO, String.format("Connecting to job master %s:%d", masterHost, masterPort));
    jobMasterClient.init();
    // now lets send the starting message
    jobMasterClient.sendWorkerStartingMessage();
    return jobMasterClient;
  }

  @Override
  public WorkerNetworkInfo getWorkerNetworkInfoForID(int id) {
    WorkerDiscoverer workerController = masterClient.getWorkerController();
    List<WorkerNetworkInfo> infos = workerController.waitForAllWorkersToJoin(1000);
    for (WorkerNetworkInfo in : infos) {
      if (in.getWorkerID() == id) {
        return in;
      }
    }
    return null;
  }

  @Override
  public int getNumberOfWorkers() {
    return numberOfWorkers;
  }

  @Override
  public List<WorkerNetworkInfo> getWorkerList() {
    WorkerDiscoverer workerController = masterClient.getWorkerController();
    return workerController.waitForAllWorkersToJoin(30000);
  }

  @Override
  public List<WorkerNetworkInfo> waitForAllWorkersToJoin(long timeLimit) {
    WorkerDiscoverer workerController = masterClient.getWorkerController();
    return workerController.waitForAllWorkersToJoin(30000);
  }
}
