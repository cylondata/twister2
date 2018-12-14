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
package edu.iu.dsc.tws.master.server;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.net.tcp.Progress;
import edu.iu.dsc.tws.common.net.tcp.StatusCode;
import edu.iu.dsc.tws.common.net.tcp.request.ConnectHandler;
import edu.iu.dsc.tws.common.net.tcp.request.RRServer;
import edu.iu.dsc.tws.master.HTGClientMonitor;
import edu.iu.dsc.tws.master.IJobTerminator;
import edu.iu.dsc.tws.master.JobMasterContext;
import edu.iu.dsc.tws.master.dashclient.DashboardClient;
import edu.iu.dsc.tws.master.dashclient.models.JobState;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI.ListWorkersRequest;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI.ListWorkersResponse;
import edu.iu.dsc.tws.proto.system.job.JobAPI;

/**
 * JobMaster class
 * It is started for each Twister2 job
 * It provides:
 *   worker discovery
 *   barrier method
 *   Ping service
 *
 * It can be started in two different modes:
 *   Threaded and Blocking
 *
 * If the user calls:
 *   startJobMasterThreaded()
 * It starts as a Thread and the call to this method returns
 *
 * If the user calls:
 *   startJobMasterBlocking()
 * It uses the calling thread and this call does not return unless the JobMaster completes
 *
 * JobMaster to Dashboard messaging
 * JobMaster reports to Dashboard server when dashboard address is provided in the config
 * If dashboard host address is not provided, it does not try to connect to dashboard server
 */

public class JobMaster {
  private static final Logger LOG = Logger.getLogger(JobMaster.class.getName());

  /**
   * an id to be used when comminicating with workers and the client
   */
  public static final int JOB_MASTER_ID = -10;

  /**
   * A singleton Progress object monitors network channel
   */
  private static Progress looper;

  /**
   * config object for the Job Master
   */
  private Config config;

  /**
   * the ip address of this job master
   */
  private String masterAddress;

  /**
   * port number of this job master
   */
  private int masterPort;

  /**
   * name of the job this Job Master will manage
   */
  private JobAPI.Job job;

  /**
   * the network object to receive and send messages
   */
  private RRServer rrServer;

  /**
   * the object to monitor workers
   */
  private WorkerMonitor workerMonitor;

  /**
   * a flag to show that whether the job is done
   * when it is converted to true, the job master exits
   */
  private boolean workersCompleted = false;

  /**
   * Job Terminator object.
   * it will the terminate all workers and cleanup job resources.
   */
  private IJobTerminator jobTerminator;

  /**
   * Number of workers expected
   */
  private int numberOfWorkers;

  /**
   * NodeInfo object for Job Master
   * location of Job Master
   */
  private JobMasterAPI.NodeInfo nodeInfo;

  /**
   * a UUID generated for each job
   * it is primarily used when communicating with Dashboard
   */
  private String jobID;

  /**
   * host address of Dashboard server
   * if it is set, job master will report to Dashboard
   * otherwise, it will ignore Dashboard
   */
  private String dashboardHost;

  /**
   * the client that will handle Dashboard messaging
   */
  private DashboardClient dashClient;

  /**
   * BarrierMonitor object
   */
  private BarrierMonitor barrierMonitor;

  //Newly Added for HTG Integration
  private HTGClientMonitor htgClientMonitor;

  private boolean htgClientsCompleted = false;

  public JobMaster(Config config,
                   String masterAddress,
                   int port,
                   IJobTerminator jobTerminator,
                   JobAPI.Job job,
                   JobMasterAPI.NodeInfo nodeInfo) {
    this.config = config;
    this.masterAddress = masterAddress;
    this.jobTerminator = jobTerminator;
    this.job = job;
    this.nodeInfo = nodeInfo;
    this.masterPort = port;
    this.numberOfWorkers = job.getNumberOfWorkers();
  }

  public JobMaster(Config config,
                   String masterAddress,
                   IJobTerminator jobTerminator,
                   JobAPI.Job job,
                   JobMasterAPI.NodeInfo nodeInfo) {
    this.config = config;
    this.masterAddress = masterAddress;
    this.jobTerminator = jobTerminator;
    this.job = job;
    this.nodeInfo = nodeInfo;
    this.masterPort = JobMasterContext.jobMasterPort(config);
    this.numberOfWorkers = job.getNumberOfWorkers();

    this.jobID = UUID.randomUUID().toString();
    this.dashboardHost = JobMasterContext.dashboardHost(config);
    if (dashboardHost == null) {
      LOG.warning("Dashboard host address is null. Not connecting to Dashboard");
      this.dashClient = null;
    } else {
      this.dashClient = new DashboardClient(dashboardHost, jobID);
    }
  }

  /**
   * initialize the Job Master
   */
  private void init() {

    looper = new Progress();

    // if Dashboard is used, register this job with that
    if (dashClient != null) {
      boolean registered = dashClient.registerJob(job, nodeInfo);
      if (!registered) {
        LOG.warning("Not using Dashboard since it can not register with it.");
        dashClient = null;
      }
    }

    ServerConnectHandler connectHandler = new ServerConnectHandler();
    rrServer = new RRServer(config, masterAddress, masterPort, looper, JOB_MASTER_ID,
        connectHandler, JobMasterContext.jobMasterAssignsWorkerIDs(config));

    workerMonitor = new WorkerMonitor(this, rrServer, dashClient, job,
        JobMasterContext.jobMasterAssignsWorkerIDs(config));

    barrierMonitor = new BarrierMonitor(numberOfWorkers, rrServer);

    //newly added for HTG Integration
    htgClientMonitor = new HTGClientMonitor(this, rrServer);

    JobMasterAPI.Ping.Builder pingBuilder = JobMasterAPI.Ping.newBuilder();

    JobMasterAPI.RegisterWorker.Builder registerWorkerBuilder =
        JobMasterAPI.RegisterWorker.newBuilder();
    JobMasterAPI.RegisterWorkerResponse.Builder registerWorkerResponseBuilder
        = JobMasterAPI.RegisterWorkerResponse.newBuilder();

    JobMasterAPI.WorkerStateChange.Builder stateChangeBuilder =
        JobMasterAPI.WorkerStateChange.newBuilder();
    JobMasterAPI.WorkerStateChangeResponse.Builder stateChangeResponseBuilder
        = JobMasterAPI.WorkerStateChangeResponse.newBuilder();

    ListWorkersRequest.Builder listWorkersBuilder = ListWorkersRequest.newBuilder();
    ListWorkersResponse.Builder listResponseBuilder = ListWorkersResponse.newBuilder();
    JobMasterAPI.BarrierRequest.Builder barrierRequestBuilder =
        JobMasterAPI.BarrierRequest.newBuilder();
    JobMasterAPI.BarrierResponse.Builder barrierResponseBuilder =
        JobMasterAPI.BarrierResponse.newBuilder();

    JobMasterAPI.ScaledComputeResource.Builder scaleMessageBuilder =
        JobMasterAPI.ScaledComputeResource.newBuilder();
    JobMasterAPI.ScaledResponse.Builder scaleResponseBuilder
        = JobMasterAPI.ScaledResponse.newBuilder();

    rrServer.registerRequestHandler(pingBuilder, workerMonitor);
    rrServer.registerRequestHandler(registerWorkerBuilder, workerMonitor);
    rrServer.registerRequestHandler(registerWorkerResponseBuilder, workerMonitor);
    rrServer.registerRequestHandler(stateChangeBuilder, workerMonitor);
    rrServer.registerRequestHandler(stateChangeResponseBuilder, workerMonitor);
    rrServer.registerRequestHandler(listWorkersBuilder, workerMonitor);
    rrServer.registerRequestHandler(listResponseBuilder, workerMonitor);
    rrServer.registerRequestHandler(barrierRequestBuilder, barrierMonitor);
    rrServer.registerRequestHandler(barrierResponseBuilder, barrierMonitor);
    rrServer.registerRequestHandler(scaleMessageBuilder, workerMonitor);
    rrServer.registerRequestHandler(scaleResponseBuilder, workerMonitor);

    //Newly added for HTG Testing
    JobMasterAPI.HTGJobRequest.Builder htgjobRequestBuilder
        = JobMasterAPI.HTGJobRequest.newBuilder();
    JobMasterAPI.HTGJobResponse.Builder htgjobResponseBuilder
        = JobMasterAPI.HTGJobResponse.newBuilder();

    rrServer.registerRequestHandler(htgjobRequestBuilder, htgClientMonitor);
    rrServer.registerRequestHandler(htgjobResponseBuilder, htgClientMonitor);

    rrServer.start();
    looper.loop();

  }

  /**
   * start the Job Master in a Thread
   */
  public Thread startJobMasterThreaded() {
    // first call the init method
    init();

    Thread jmThread = new Thread() {
      public void run() {
        startLooping();
      }
    };

    jmThread.start();

    return jmThread;
  }

  /**
   * start the Job Master in a blocking call
   */
  public void startJobMasterBlocking() {
    // first call the init method
    init();

    startLooping();
  }

  /**
   * Job Master loops until all workers in the job completes
   */
  private void startLooping() {
    LOG.info("JobMaster [" + masterAddress + "] started and waiting worker messages on port: "
        + masterPort);

    while (!workersCompleted) {
      looper.loopBlocking();
    }

    //Newly Added for HTG Integration
    while (!htgClientsCompleted) {
      looper.loopBlocking();
    }

    // send the remaining messages if any and stop
    rrServer.stopGraceFully(2000);
  }

  /**
   * this method is called when all workers became RUNNING
   * we let the dashboard know that the job STARTED
   */
  public void allWorkersBecameRunning() {
    // if Dashboard is used, tell it that the job has STARTED
    if (dashClient != null) {
      dashClient.jobStateChange(JobState.STARTED);
    }
  }

  /**
   * this method is executed when the worker completed message received from all workers
   */
  public void allWorkersCompleted() {

    // if Dashboard is used, tell it that the job has completed
    if (dashClient != null) {
      dashClient.jobStateChange(JobState.COMPLETED);
    }

    LOG.info("All " + numberOfWorkers + " workers have completed. JobMaster is stopping.");
    workersCompleted = true;
    looper.wakeup();

    if (jobTerminator != null) {
      jobTerminator.terminateJob(job.getJobName());
    }
  }

  //Newly Added for HTG Integration
  public void allHTGClientsCompleted() {
    htgClientsCompleted = true;
    looper.wakeup();
  }

  public void addShutdownHook() {
    Thread hookThread = new Thread() {
      public void run() {
        allWorkersCompleted();
      }
    };

    Runtime.getRuntime().addShutdownHook(hookThread);
  }

  public class ServerConnectHandler implements ConnectHandler {
    @Override
    public void onError(SocketChannel channel) {
    }

    @Override
    public void onConnect(SocketChannel channel, StatusCode status) {
      try {
        LOG.fine("Client connected from:" + channel.getRemoteAddress());
      } catch (IOException e) {
        LOG.log(Level.SEVERE, "Exception when getting RemoteAddress", e);
      }
    }

    @Override
    public void onClose(SocketChannel channel) {
    }
  }

}

