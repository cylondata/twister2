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
package edu.iu.dsc.tws.rsched.schedulers.mesos;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.discovery.IWorkerController;
import edu.iu.dsc.tws.common.discovery.NodeInfo;
import edu.iu.dsc.tws.common.discovery.WorkerNetworkInfo;
import edu.iu.dsc.tws.proto.system.job.JobAPI;
import edu.iu.dsc.tws.rsched.bootstrap.ZKWorkerController;


public class MesosWorkerController implements IWorkerController {

  public static final Logger LOG = Logger.getLogger(MesosWorkerController.class.getName());
  private Config config;
  private String jobName;
  private JobAPI.Job job;
  private String workerIp;
  private int workerPort;
  private int numberOfWorkers;
  private int containerPerWorker;
  private List<WorkerNetworkInfo> workerList;
  private WorkerNetworkInfo thisWorker;
  private ZKWorkerController zkWorkerController;

  public MesosWorkerController(Config cfg, JobAPI.Job job, String ip, int port, int workerID) {
    config = cfg;
    this.jobName = job.getJobName();
    this.job = job;
    this.workerIp = ip;
    this.workerPort = port;
    numberOfWorkers = MesosContext.numberOfContainers(config) - 1;
    containerPerWorker = MesosContext.containerPerWorker(config);
    workerList = new ArrayList<>();
    thisWorker = new WorkerNetworkInfo(convertStringToIP(ip), port, workerID,
        new NodeInfo(ip, "rack1", "dc1"));
  }

  /**
   * covert the given string to ip address object
   */
  private InetAddress convertStringToIP(String ipStr) {
    try {
      return InetAddress.getByName(ipStr);
    } catch (UnknownHostException e) {
      LOG.log(Level.SEVERE, "Can not convert the IP to InetAddress: " + ipStr, e);
      throw new RuntimeException(e);
    }
  }

  @Override
  public WorkerNetworkInfo getWorkerNetworkInfo() {
    return thisWorker;
  }

  @Override
  public WorkerNetworkInfo getWorkerNetworkInfoForID(int id) {
    return null;
  }

  @Override
  public int getNumberOfWorkers() {
    return zkWorkerController.getNumberOfWorkers();
  }

  @Override
  public List<WorkerNetworkInfo> getWorkerList() {
    return zkWorkerController.getWorkerList();
  }


  public void initializeWithZooKeeper() {

    long startTime = System.currentTimeMillis();
    String workerHostPort = workerIp + ":" + workerPort;

    // temporary value
    NodeInfo nodeInfo = new NodeInfo(workerIp, null, null);
    zkWorkerController =
        new ZKWorkerController(config, job.getJobName(), workerHostPort, numberOfWorkers, nodeInfo);
    zkWorkerController.initialize();
    long duration = System.currentTimeMillis() - startTime;
    LOG.info("Initialization for the worker: " + zkWorkerController.getWorkerNetworkInfo()
        + " took: " + duration + "ms");
  }

  @Override
  public List<WorkerNetworkInfo> waitForAllWorkersToJoin(long timeLimitMilliSec) {

    LOG.info("Waiting for " + numberOfWorkers + " workers to join .........");

    // the amount of time to wait for all workers to join a job
    //int timeLimit =  ZKContext.maxWaitTimeForAllWorkersToJoin(config);
    long startTime = System.currentTimeMillis();
    workerList = zkWorkerController.waitForAllWorkersToJoin(timeLimitMilliSec);
    long duration = System.currentTimeMillis() - startTime;

    if (workerList == null) {
      LOG.log(Level.SEVERE, "Could not get full worker list. timeout limit has been reached !!!!"
          + "Waited " + timeLimitMilliSec + " ms.");
    } else {
      LOG.log(Level.INFO, "Waited " + duration + " ms for all workers to join.");

      workerList = zkWorkerController.getWorkerList();
      LOG.info("list of current workers in the job: ");
      zkWorkerController.printWorkers(workerList);

      LOG.info("list of all joined workers to the job: ");
      zkWorkerController.printWorkers(zkWorkerController.getWorkerList());

    }
    return workerList;
  }

  @Override
  public boolean waitOnBarrier(long timeLimitMilliSec) {
    return zkWorkerController.waitOnBarrier(timeLimitMilliSec);
  }

  /**
   * needs to close down when finished computation
   */
  public void close() {
    zkWorkerController.close();
  }
}
