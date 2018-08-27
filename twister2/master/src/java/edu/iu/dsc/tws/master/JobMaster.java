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
package edu.iu.dsc.tws.master;

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.net.tcp.Progress;
import edu.iu.dsc.tws.common.net.tcp.StatusCode;
import edu.iu.dsc.tws.common.net.tcp.request.ConnectHandler;
import edu.iu.dsc.tws.common.net.tcp.request.RRServer;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI.ListWorkersRequest;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI.ListWorkersResponse;

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
 */

public class JobMaster {
  private static final Logger LOG = Logger.getLogger(JobMaster.class.getName());

  /**
   * Job Master ID is assigned as -1,
   * workers will have IDs starting from 0 and icreasing by one
   */
  public static final int JOB_MASTER_ID = -1;

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
  private String jobName;

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
   * BarrierMonitor object
   */
  private BarrierMonitor barrierMonitor;

  public JobMaster(Config config,
                   String masterAddress,
                   IJobTerminator jobTerminator,
                   String jobName) {
    this(config, masterAddress, jobTerminator, jobName, JobMasterContext.jobMasterPort(config),
        JobMasterContext.workerInstances(config));
  }

  public JobMaster(Config config,
                   String masterAddress,
                   IJobTerminator jobTerminator,
                   String jobName,
                   int masterPort,
                   int numWorkers) {
    this.config = config;
    this.masterAddress = masterAddress;
    this.jobTerminator = jobTerminator;
    this.jobName = jobName;
    this.masterPort = masterPort;
    this.numberOfWorkers = numWorkers;
  }

  /**
   * initialize the Job Master
   */
  private void init() {

    looper = new Progress();

    ServerConnectHandler connectHandler = new ServerConnectHandler();
    rrServer =
        new RRServer(config, masterAddress, masterPort, looper, JOB_MASTER_ID, connectHandler);

    workerMonitor = new WorkerMonitor(config, this, rrServer, numberOfWorkers);
    barrierMonitor = new BarrierMonitor(numberOfWorkers, rrServer);

    JobMasterAPI.Ping.Builder pingBuilder = JobMasterAPI.Ping.newBuilder();
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

    rrServer.registerRequestHandler(pingBuilder, workerMonitor);
    rrServer.registerRequestHandler(stateChangeBuilder, workerMonitor);
    rrServer.registerRequestHandler(stateChangeResponseBuilder, workerMonitor);
    rrServer.registerRequestHandler(listWorkersBuilder, workerMonitor);
    rrServer.registerRequestHandler(listResponseBuilder, workerMonitor);
    rrServer.registerRequestHandler(barrierRequestBuilder, barrierMonitor);
    rrServer.registerRequestHandler(barrierResponseBuilder, barrierMonitor);

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

    // send the remaining messages if any and stop
    rrServer.stopGraceFully(2000);
  }

  /**
   * this method is executed when the worker completed message received from all workers
   */
  public void allWorkersCompleted() {

    LOG.info("All workers have completed. JobMaster is stopping.");
    workersCompleted = true;
    looper.wakeup();

    if (jobTerminator != null) {
      jobTerminator.terminateJob(jobName);
    }
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
        LOG.info("Client connected from:" + channel.getRemoteAddress());
      } catch (IOException e) {
        LOG.log(Level.SEVERE, "Exception when getting RemoteAddress", e);
      }
    }

    @Override
    public void onClose(SocketChannel channel) {
    }
  }

}
