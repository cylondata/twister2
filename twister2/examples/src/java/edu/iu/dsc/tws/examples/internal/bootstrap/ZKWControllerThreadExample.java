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

import edu.iu.dsc.tws.api.Twister2Job;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.common.logging.LoggingHelper;
import edu.iu.dsc.tws.common.zk.ZKWorkerController;
import edu.iu.dsc.tws.examples.basic.HelloWorld;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.proto.utils.ComputeResourceUtils;
import edu.iu.dsc.tws.proto.utils.NodeInfoUtils;
import edu.iu.dsc.tws.proto.utils.WorkerInfoUtils;

/**
 * ZKWorkerController threaded example
 * This may be broken. Since the connection in ZKWorkerController is singleton now
 */
public final class ZKWControllerThreadExample extends Thread {
  public static final Logger LOG = Logger.getLogger(ZKWControllerThreadExample.class.getName());

  public static String jobName;
  public static Config config;
  public static int numberOfWorkers;

  private int workerID;

  public ZKWControllerThreadExample(int workerID) {
    this.workerID = workerID;
  }

  @Override
  public void run() {
    try {
      simulateWorker();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /**
   * an example usage of ZKWorkerController class
   */
  public void simulateWorker()
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

    ZKWorkerController jobController =
        new ZKWorkerController(config, jobName, numberOfWorkers, workerInfo);

    jobController.initialize();

    List<JobMasterAPI.WorkerInfo> workerList = jobController.getJoinedWorkers();
    LOG.info("Initial worker list: \n" + WorkerInfoUtils.workerListAsString(workerList));

    LOG.info("Waiting for all workers to join: ");
    // wait until 100sec
    workerList = jobController.getAllWorkers();
    LOG.info(WorkerInfoUtils.workerListAsString(workerList));

    // test state change
    jobController.updateWorkerStatus(JobMasterAPI.WorkerState.RUNNING);
    ZKWControllerExample.sleeeep((long) (Math.random() * 10000));

    // test worker failure
    // assume this worker failed
    // it does not send COMPLETE message, before leaving
    if (workerID == 1) {
      jobController.close();
    }

    LOG.info("Waiting on the first barrier -------------------------- ");

    jobController.waitOnBarrier();
    LOG.info("All workers reached the barrier. Proceeding.");

    workerList = jobController.getAllWorkers();
    LOG.info("Workers after first barrier: \n" + WorkerInfoUtils.workerListAsString(workerList));

    jobController.updateWorkerStatus(JobMasterAPI.WorkerState.COMPLETED);

    // sleep some random amount of time before closing
    // this is to prevent all workers to close almost at the same time
    ZKWControllerExample.sleeeep((long) (Math.random() * 2000));
    jobController.close();
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
    numberOfWorkers = Integer.parseInt(args[1]);
    jobName = "test-job";

    config = ZKWControllerExample.buildTestConfig(zkAddress);


    LoggingHelper.initTwisterFileLogHandler(numberOfWorkers + "", "logs", config);

    Twister2Job twister2Job = Twister2Job.newBuilder()
        .setJobName(jobName)
        .setWorkerClass(HelloWorld.class)
        .addComputeResource(2, 1024, numberOfWorkers)
        .build();
    JobAPI.Job job = twister2Job.serialize();

    ZKWControllerExample.createJobZnode(job);

    for (int i = 0; i < numberOfWorkers; i++) {
      new ZKWControllerThreadExample(i).start();
    }

  }


  public static void printUsage() {
    LOG.info("Usage:\n"
        + "java ZKWorkerControllerExample zkAddress numberOfWorkers\n"
        + "\tzkAddress is in the form of IP:PORT\n"
        + "\tnumberOfWorkers to be created in the job.");
  }


}

