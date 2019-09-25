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
package edu.iu.dsc.tws.examples.internal.bootstrap;

import java.net.InetAddress;
import java.util.List;
import java.util.logging.Logger;

import org.apache.curator.framework.CuratorFramework;

import edu.iu.dsc.tws.api.Twister2Job;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.common.logging.LoggingHelper;
import edu.iu.dsc.tws.examples.basic.HelloWorld;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.proto.utils.ComputeResourceUtils;
import edu.iu.dsc.tws.proto.utils.NodeInfoUtils;
import edu.iu.dsc.tws.proto.utils.WorkerInfoUtils;
import edu.iu.dsc.tws.rsched.bootstrap.ZKContext;
import edu.iu.dsc.tws.rsched.zk.ZKJobController;
import edu.iu.dsc.tws.rsched.zk.ZKJobZnodeUtil;
import edu.iu.dsc.tws.rsched.zk.ZKUtils;
import static java.lang.Thread.sleep;

public final class ZKJobControllerExample {
  public static final Logger LOG = Logger.getLogger(ZKJobControllerExample.class.getName());

  private ZKJobControllerExample() { }

  public static String jobName;
  public static Config config;
  public static int numberOfWorkers;

  public static void main(String[] args) throws Exception {

    if (args.length < 2) {
      printUsage();
      return;
    }

    String zkAddress = args[0];
    String action = args[1];
    jobName = "test-job";

    config = ZKJobControllerExample.buildTestConfig(zkAddress);

    if ("delete".equalsIgnoreCase(action)) {
      deleteJobZnode();
    } else if ("join".equalsIgnoreCase(action)) {
      numberOfWorkers = Integer.parseInt(args[2]);
      int workerID = Integer.parseInt(args[3]);

      LoggingHelper.initTwisterFileLogHandler(workerID + "", "logs", config);

      Twister2Job twister2Job = Twister2Job.newBuilder()
          .setJobName(jobName)
          .setWorkerClass(HelloWorld.class)
          .addComputeResource(2, 1024, numberOfWorkers)
          .build();
      JobAPI.Job job = twister2Job.serialize();
//      createJobZnode(job);

      simulateWorker(workerID);

    }

  }

  /**
   * an example usage of ZKWorkerController class
   */
  public static void simulateWorker(int workerID) throws Exception {

    int port = 1000 + (int) (Math.random() * 1000);
    String workerIP;
    workerIP = InetAddress.getLocalHost().getHostAddress();

    JobMasterAPI.NodeInfo nodeInfo =
        NodeInfoUtils.createNodeInfo("node1.on.hostx", "rack1", "dc01");
    JobAPI.ComputeResource computeResource =
        ComputeResourceUtils.createComputeResource(0, 1, 1024, 2);

    JobMasterAPI.WorkerInfo workerInfo =
        WorkerInfoUtils.createWorkerInfo(workerID, workerIP, port);
//      WorkerInfoUtils.createWorkerInfo(workerID, workerIP, port, nodeInfo, computeResource);

    LOG.info("workerInfo at example: " + workerInfo.toString());

    ZKJobController jobController =
        new ZKJobController(config, jobName, numberOfWorkers, workerInfo);

    jobController.initialize();

    List<JobMasterAPI.WorkerInfo> workerList = jobController.getJoinedWorkers();
    LOG.info("Initial worker list: \n" + WorkerInfoUtils.workerListAsString(workerList));

    LOG.info("Waiting for all workers to join: ");
    // wait until 100sec
    workerList = jobController.getAllWorkers();
    LOG.info(WorkerInfoUtils.workerListAsString(workerList));

    // test state change
    jobController.updateWorkerState(JobMasterAPI.WorkerState.RUNNING);
    sleeeep((long) (Math.random() * 10000));

    // test worker failure
    // assume this worker failed
    // it does not send COMPLETE message, before leaving
    if (workerID == 1) {
      throw new RuntimeException();
    }

    LOG.info("Waiting on the first barrier -------------------------- ");

    jobController.waitOnBarrier();
    LOG.info("All workers reached the barrier. Proceeding.");

    workerList = jobController.getAllWorkers();
    LOG.info("Workers after first barrier: \n" + WorkerInfoUtils.workerListAsString(workerList));

    jobController.updateWorkerState(JobMasterAPI.WorkerState.COMPLETED);

    // sleep some random amount of time before closing
    // this is to prevent all workers to close almost at the same time
    sleeeep((long) (Math.random() * 2000));
    jobController.close();
  }


  /**
   * construct a test Config object
   */
  public static Config buildTestConfig(String zkAddresses) {

    return Config.newBuilder()
        .put(ZKContext.ZOOKEEPER_SERVER_ADDRESSES, zkAddresses)
        .build();
  }

  public static void deleteJobZnode() {

    CuratorFramework client = ZKUtils.connectToServer(config);

    if (ZKJobZnodeUtil.isThereJobZNodes(client, jobName, config)) {
      ZKJobZnodeUtil.deleteJobZNodes(config, client, jobName);
    }

    ZKUtils.closeClient(client);
  }

  public static void sleeeep(long duration) {

    LOG.info("Sleeping " + duration + "ms .....");

    try {
      sleep(duration);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }


  public static void printUsage() {
    LOG.info("Usage:\n"
        + "java ZKWorkerControllerExample zkAddress action numberOfWorkers workerID\n"
        + "\tzkAddress is in the form of IP:PORT\n"
        + "\taction can be: join, delete\n"
        + "\tnumberOfWorkers is only needed for join\n"
        + "\tworkerID is only needed for join");
  }

}
