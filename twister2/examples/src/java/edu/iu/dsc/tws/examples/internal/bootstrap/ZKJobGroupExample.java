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

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.common.logging.LoggingHelper;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.proto.utils.ComputeResourceUtils;
import edu.iu.dsc.tws.proto.utils.NodeInfoUtils;
import edu.iu.dsc.tws.proto.utils.WorkerInfoUtils;
import edu.iu.dsc.tws.rsched.bootstrap.ZKContext;
import edu.iu.dsc.tws.rsched.bootstrap.ZKJobGroup;
import edu.iu.dsc.tws.rsched.bootstrap.ZKUtil;

public final class ZKJobGroupExample {
  public static final Logger LOG = Logger.getLogger(ZKJobGroupExample.class.getName());

  private ZKJobGroupExample() {
  }

  /**
   * example usage of ZKWorkerController class
   * Two actions supported:
   * join: join a Job znode
   * delete: delete a Job znode
   * <p>
   * First parameter is the ZooKeeper server address
   * Second parameter is the requested action
   * Third parameter is the numberOfWorkers to join the job
   * Fourth parameter is workerID for this worker
   * Third and fourth parameters are only used for join, not for delete
   * <p>
   * Sometimes, two workers may finish almost at the same time
   * in that case, both worker caches may not be updated
   * therefore, both think that they are not the last ones
   * so, the job znode may not be deleted
   * in that case, delete action can be used to delete the previous job znode
   */
  public static void main(String[] args) throws Exception {

    if (args.length < 2) {
      printUsage();
      return;
    }

    String zkAddress = args[0];
    String action = args[1];
    String jobName = "test-job";

    Config cnfg = buildTestConfig(zkAddress, jobName);

    if ("delete".equalsIgnoreCase(action)) {
      deleteJobZnode(jobName, cnfg);
    } else if ("join".equalsIgnoreCase(action)) {
      int numberOfWorkers = Integer.parseInt(args[2]);
      int workerID = Integer.parseInt(args[3]);

      LoggingHelper.initTwisterFileLogHandler(workerID + "", "logs", cnfg);
      simulateWorker(jobName, numberOfWorkers, workerID, cnfg);
    }

  }

  /**
   * construct a test Config object
   */
  public static Config buildTestConfig(String zkAddresses, String jobName) {

    return Config.newBuilder()
        .put(ZKContext.ZOOKEEPER_SERVER_ADDRESSES, zkAddresses)
        .build();
  }

  public static void printUsage() {
    LOG.info("Usage:\n"
        + "java ZKWorkerControllerExample zkAddress action numberOfWorkers\n"
        + "\tzkAddress is in the form of IP:PORT"
        + "\taction can be: join, delete\n"
        + "\tnumberOfWorkers is only needed for join");
  }

  public static void deleteJobZnode(String jobName, Config cnfg) {
    if (ZKUtil.isThereAnActiveJob(jobName, cnfg)) {
      ZKUtil.terminateJob(jobName, cnfg);
      return;
    } else {
      LOG.info("No job Znode to delete for the jobName: " + jobName);
      return;
    }
  }

  /**
   * an example usage of ZKWorkerController class
   */
  public static void simulateWorker(String jobName, int numberOfWorkers, int workerID, Config cnf)
      throws Exception {

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

    ZKJobGroup zkJobGroup = new ZKJobGroup(cnf, jobName, numberOfWorkers, workerInfo);

    zkJobGroup.initialize();

    List<JobMasterAPI.WorkerInfo> workerList = zkJobGroup.getJoinedWorkers();
    LOG.info("Initial worker list: \n" + WorkerInfoUtils.workerListAsString(workerList));

    LOG.info("Waiting for all workers to join: ");
    // wait until 100sec
    workerList = zkJobGroup.getAllWorkers();
    LOG.info(WorkerInfoUtils.workerListAsString(workerList));

    sleeeep((long) (Math.random() * 10000));

    LOG.info("Waiting on the first barrier -------------------------- ");

    zkJobGroup.waitOnBarrier();
    LOG.info("All workers reached the barrier. Proceeding.");

    workerList = zkJobGroup.getAllWorkers();
    LOG.info("Workers after first barrier: \n" + WorkerInfoUtils.workerListAsString(workerList));

    sleeeep((long) (Math.random() * 10000));

    LOG.info("Waiting on the second barrier -------------------------- ");
    zkJobGroup.waitOnBarrier();
    LOG.info("All workers reached the barrier. Proceeding.");

    // sleep some random amount of time before closing
    // this is to prevent all workers to close almost at the same time
//    sleeeep((long) (Math.random() * 2000));
    zkJobGroup.close();
  }

  public static void sleeeep(long duration) {

    LOG.info("Sleeping " + duration + "ms .....");

    try {
      Thread.sleep(duration);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

}
