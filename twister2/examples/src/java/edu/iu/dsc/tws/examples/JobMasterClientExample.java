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
package edu.iu.dsc.tws.examples;

import java.net.InetAddress;
import java.util.List;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.Context;
import edu.iu.dsc.tws.common.discovery.WorkerNetworkInfo;
import edu.iu.dsc.tws.master.JobMasterContext;
import edu.iu.dsc.tws.master.client.JobMasterClient;
import edu.iu.dsc.tws.master.client.WorkerDiscoverer;

public final class JobMasterClientExample {
  private static final Logger LOG = Logger.getLogger(JobMasterClientExample.class.getName());

  private JobMasterClientExample() { }

  /**
   * a test class to run JobMasterClient
   * First, a JobMaster instance should be started on a machine
   * This client should connect to that server
   *
   * NumberOfWorkers to join must be given as a command line parameter
   * Each worker waits for all workers to join
   *
   * When all workers joined, they get the full worker list
   * each send completed messages and exit
   *
   * @param args
   */
  public static void main(String[] args) {

    if (args.length != 1) {
      printUsage();
      return;
    }

    String jobMasterAddress = "localhost";
    int numberOfWorkers = Integer.parseInt(args[0]);

    Config config = buildConfig(jobMasterAddress, numberOfWorkers);

    int workerTempID = 0;

    simulateClient(config, workerTempID);
  }

  /**
   * a method to simulate JobMasterClient running in workers
   */
  public static void simulateClient(Config config, int workerTempID) {

    InetAddress workerIP = WorkerDiscoverer.convertStringToIP("localhost");
    int workerPort = 10000 + (int) (Math.random() * 10000);

    WorkerNetworkInfo workerNetworkInfo = new WorkerNetworkInfo(workerIP, workerPort, workerTempID);

    JobMasterClient client = new JobMasterClient(config, workerNetworkInfo);
    client.init();

    client.sendWorkerStartingMessage();

    // wait 500ms
    // sleeeep(5000);

    client.sendWorkerRunningMessage();

    List<WorkerNetworkInfo> workerList = client.getWorkerController().getWorkerList();
    LOG.info(WorkerNetworkInfo.workerListAsString(workerList));

    workerList = client.getWorkerController().waitForAllWorkersToJoin(100000);
    LOG.info(WorkerNetworkInfo.workerListAsString(workerList));

    client.sendWorkerCompletedMessage();
//    sleeeep(500);

    client.close();

    System.out.println("all messaging done. waiting before finishing.");
//    sleeeep(5000);
  }

  /**
   * construct a Config object
   * @return
   */
  public static Config buildConfig(String jobMasterAddress, int numberOfWorkers) {
    return Config.newBuilder()
        .put(Context.TWISTER2_WORKER_INSTANCES, numberOfWorkers)
        .put(JobMasterContext.PING_INTERVAL, 3000)
        .put(JobMasterContext.JOB_MASTER_IP, jobMasterAddress)
        .put(JobMasterContext.JOB_MASTER_ASSIGNS_WORKER_IDS, true)
        .build();
  }

  public static void sleeeep(long duration) {

    try {
      Thread.sleep(duration);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  public static void printUsage() {
    LOG.info("Usage:\n"
        + "java JobMasterClientExample numberOfWorkers");
  }

}
