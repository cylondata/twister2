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
import edu.iu.dsc.tws.api.config.Context;
import edu.iu.dsc.tws.api.resource.IAllJoinedListener;
import edu.iu.dsc.tws.api.resource.IJobMasterListener;
import edu.iu.dsc.tws.api.resource.IWorkerController;
import edu.iu.dsc.tws.api.resource.IWorkerFailureListener;
import edu.iu.dsc.tws.api.resource.IWorkerStatusUpdater;
import edu.iu.dsc.tws.common.logging.LoggingHelper;
import edu.iu.dsc.tws.common.zk.ZKContext;
import edu.iu.dsc.tws.common.zk.ZKJobZnodeUtil;
import edu.iu.dsc.tws.common.zk.ZKUtils;
import edu.iu.dsc.tws.common.zk.ZKWorkerController;
import edu.iu.dsc.tws.examples.basic.HelloWorld;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.proto.utils.ComputeResourceUtils;
import edu.iu.dsc.tws.proto.utils.NodeInfoUtils;
import edu.iu.dsc.tws.proto.utils.WorkerInfoUtils;
import static java.lang.Thread.sleep;

/**
 * ZKWorkerController example
 */

public final class ZKWControllerExample {
  public static final Logger LOG = Logger.getLogger(ZKWControllerExample.class.getName());

  private ZKWControllerExample() { }

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

    config = ZKWControllerExample.buildTestConfig(zkAddress);

    if ("delete".equalsIgnoreCase(action)) {
      deleteJobZnode();
      return;
    }

    // get numberOfWorkers
    numberOfWorkers = Integer.parseInt(args[2]);

    if ("create".equalsIgnoreCase(action)) {
      JobAPI.Job job = buildJob();
      createJobZnode(config, job);

    } else if ("update".equalsIgnoreCase(action)) {
      JobAPI.Job job = buildJob();
      updateJobZnode(job);

    } else if ("join".equalsIgnoreCase(action)) {
      int workerID = Integer.parseInt(args[3]);

      LoggingHelper.initTwisterFileLogHandler(workerID + "", "logs", config);
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

    ZKWorkerController zkWorkerController =
        new ZKWorkerController(config, jobName, numberOfWorkers, workerInfo);

    IWorkerController workerController = zkWorkerController;
    IWorkerStatusUpdater workerStatusUpdater = zkWorkerController;

    zkWorkerController.addFailureListener(new IWorkerFailureListener() {
      @Override
      public void failed(int workerID) {
        LOG.info(String.format("Worker[%s] failed.......................................",
            workerID));
      }

      @Override
      public void restarted(JobMasterAPI.WorkerInfo workerInfo) {
        LOG.info(String.format("Worker[%s] has come back from failure ......................",
            workerInfo.getWorkerID()));
      }
    });

    zkWorkerController.addAllJoinedListener(new IAllJoinedListener() {
      @Override
      public void allWorkersJoined(List<JobMasterAPI.WorkerInfo> workerList) {
        LOG.info("**************** All workers joined. Number of workers: " + workerList.size());
      }
    });

    zkWorkerController.addJobMasterListener(new IJobMasterListener() {
      @Override
      public void jobMasterJoined(String jobMasterAddress) {
        LOG.info("jobMaster joined: " + jobMasterAddress);
      }

      @Override
      public void jobMasterFailed() {
        LOG.info("jobMaster failed................................... ");
      }

      @Override
      public void jobMasterRejoined(String jobMasterAddress) {
        LOG.info("jobMaster rejoined: " + jobMasterAddress);
      }
    });

    zkWorkerController.initialize(JobMasterAPI.WorkerState.STARTED);

    List<JobMasterAPI.WorkerInfo> workerList = workerController.getJoinedWorkers();
    LOG.info("Initial worker list: \n" + WorkerInfoUtils.workerListAsString(workerList));

    LOG.info("Waiting for all workers to join: ");
    // wait until 100sec
    workerList = workerController.getAllWorkers();
    LOG.info(WorkerInfoUtils.workerListAsString(workerList));

    sleeeep((long) (Math.random() * 10 * 1000));

    // test worker failure
    // assume this worker failed
    // it does not send COMPLETE message, before leaving
//    if (workerID == 1) {
//      throw new RuntimeException();
//    }

    LOG.info(workerID + ": Waiting on the first barrier -------------------------- ");

    workerController.waitOnBarrier();
    LOG.info("All workers reached the barrier. Proceeding.");

    workerList = workerController.getAllWorkers();
    LOG.info("Workers after first barrier: \n" + WorkerInfoUtils.workerListAsString(workerList));

    workerStatusUpdater.updateWorkerStatus(JobMasterAPI.WorkerState.COMPLETED);

    zkWorkerController.close();
  }

  public static JobAPI.Job buildJob() {

    Twister2Job twister2Job = Twister2Job.newBuilder()
        .setJobName(jobName)
        .setJobID(config.getStringValue(Context.JOB_ID))
        .setWorkerClass(HelloWorld.class)
        .addComputeResource(2, 1024, numberOfWorkers)
        .build();
    JobAPI.Job job = twister2Job.serialize();
    return job;
  }

  /**
   * construct a test Config object
   */
  public static Config buildTestConfig(String zkAddresses) {

    config = Config.newBuilder()
        .put(ZKContext.SERVER_ADDRESSES, zkAddresses)
        .build();

    return config;
  }

  public static void createJobZnode(Config conf, JobAPI.Job job) {

    CuratorFramework client = ZKUtils.connectToServer(ZKContext.serverAddresses(conf));
    String rootPath = ZKContext.rootNode(conf);

    if (ZKJobZnodeUtil.isThereJobZNodes(client, rootPath, job.getJobName())) {
      ZKJobZnodeUtil.deleteJobZNodes(client, rootPath, job.getJobName());
    }

    try {
      ZKJobZnodeUtil.createJobZNode(client, rootPath, job);

      // test job znode content reading
      JobAPI.Job readJob = ZKJobZnodeUtil.readJobZNodeBody(client, job.getJobName(), conf);
      LOG.info("JobZNode content: " + readJob);

    } catch (Exception e) {
      e.printStackTrace();
    }

    ZKUtils.closeClient();
  }

  public static void deleteJobZnode() {

    CuratorFramework client = ZKUtils.connectToServer(ZKContext.serverAddresses(config));

    if (ZKJobZnodeUtil.isThereJobZNodes(client, ZKContext.rootNode(config), jobName)) {
      ZKJobZnodeUtil.deleteJobZNodes(client, ZKContext.rootNode(config), jobName);
    }

    ZKUtils.closeClient();
  }

  public static void updateJobZnode(JobAPI.Job job) throws Exception {

    CuratorFramework client = ZKUtils.connectToServer(ZKContext.serverAddresses(config));
    String jobPath = ZKUtils.constructWorkersEphemDir(ZKContext.rootNode(config), jobName);
    ZKJobZnodeUtil.updateJobZNode(client, job, jobPath);

    ZKUtils.closeClient();
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
