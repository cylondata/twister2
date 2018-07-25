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
import edu.iu.dsc.tws.proto.network.Network;
import edu.iu.dsc.tws.proto.network.Network.ListWorkersRequest;
import edu.iu.dsc.tws.proto.network.Network.ListWorkersResponse;

public class JobMaster extends Thread {
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

  public void init() {

    looper = new Progress();

    ServerConnectHandler connectHandler = new ServerConnectHandler();
    rrServer =
        new RRServer(config, masterAddress, masterPort, looper, JOB_MASTER_ID, connectHandler);

    workerMonitor = new WorkerMonitor(config, this, rrServer, numberOfWorkers);

    Network.Ping.Builder pingBuilder = Network.Ping.newBuilder();
    Network.WorkerStateChange.Builder stateChangeBuilder = Network.WorkerStateChange.newBuilder();
    Network.WorkerStateChangeResponse.Builder stateChangeResponseBuilder
        = Network.WorkerStateChangeResponse.newBuilder();

    ListWorkersRequest.Builder listWorkersBuilder = ListWorkersRequest.newBuilder();
    ListWorkersResponse.Builder listResponseBuilder = ListWorkersResponse.newBuilder();

    rrServer.registerRequestHandler(pingBuilder, workerMonitor);
    rrServer.registerRequestHandler(stateChangeBuilder, workerMonitor);
    rrServer.registerRequestHandler(stateChangeResponseBuilder, workerMonitor);
    rrServer.registerRequestHandler(listWorkersBuilder, workerMonitor);
    rrServer.registerRequestHandler(listResponseBuilder, workerMonitor);

    rrServer.start();
    looper.loop();

    start();
  }

  @Override
  public void run() {

    LOG.info("JobMaster [" + masterAddress + "] started and waiting worker messages on port: "
        + masterPort);

    while (!workersCompleted) {
      looper.loopBlocking();
    }

    // to send the last remaining messages if any
//    looper.sendQueedMessages();
    looper.loop();
    looper.loop();
    looper.loop();

    rrServer.stop();
  }

  /**
   * this method is executed when the worker completed message received from all workers
   */
  public void allWorkersCompleted() {

    LOG.info("All workers have completed. JobMaster will stop.");
    workersCompleted = true;
    looper.wakeup();

    if (jobTerminator != null) {
      jobTerminator.terminateJob(jobName);
    }
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
