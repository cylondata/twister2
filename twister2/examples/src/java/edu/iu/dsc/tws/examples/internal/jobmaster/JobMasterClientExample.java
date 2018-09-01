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
package edu.iu.dsc.tws.examples.internal.jobmaster;

import java.net.InetAddress;
import java.util.List;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.config.Context;
import edu.iu.dsc.tws.common.discovery.IWorkerController;
import edu.iu.dsc.tws.common.discovery.NodeInfo;
import edu.iu.dsc.tws.common.discovery.WorkerNetworkInfo;
import edu.iu.dsc.tws.master.JobMasterContext;
import edu.iu.dsc.tws.master.client.JMWorkerController;
import edu.iu.dsc.tws.master.client.JobMasterClient;

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
   * Then, each worker sends a barrier message
   * Then, each worker sends a completed message and closes
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

    InetAddress workerIP = JMWorkerController.convertStringToIP("localhost");
    int workerPort = 10000 + (int) (Math.random() * 10000);

    NodeInfo nodeInfo = new NodeInfo("node.ip", "rack01", null);
    WorkerNetworkInfo workerNetworkInfo =
        new WorkerNetworkInfo(workerIP, workerPort, workerTempID, nodeInfo);

    JobMasterClient client = new JobMasterClient(config, workerNetworkInfo);
    Thread clientThread = client.startThreaded();
    if (clientThread == null) {
      LOG.severe("JobMasterClient can not initialize. Exiting ...");
      return;
    }

    IWorkerController workerController = client.getJMWorkerController();

    client.sendWorkerStartingMessage();

    // wait up to 2sec
    sleeeep((long) (Math.random() * 2000));

    client.sendWorkerRunningMessage();

    List<WorkerNetworkInfo> workerList = workerController.getWorkerList();
    LOG.info(WorkerNetworkInfo.workerListAsString(workerList));

    workerList = workerController.waitForAllWorkersToJoin(100000);
    LOG.info(WorkerNetworkInfo.workerListAsString(workerList));

    // wait up to 10sec
    sleeeep((long) (Math.random() * 10000));
    long timeLimit = 20000;
    boolean allWorkersReachedBarrier = client.getJMWorkerController().waitOnBarrier(timeLimit);
    if (allWorkersReachedBarrier) {
      LOG.info("All workers reached the barrier. Proceeding.");
    } else {
      LOG.info("Not all workers reached the barrier on the given timelimit: " + timeLimit + "ms"
          + " Exiting ....... ");
      client.close();
      return;
    }

    // wait up to 3sec
    sleeeep((long) (Math.random() * 3000));

    client.sendWorkerCompletedMessage();

    client.close();

    System.out.println("Client has finished the computation. Client exiting.");
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

    LOG.info("Sleeping " + duration + "ms............");

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
