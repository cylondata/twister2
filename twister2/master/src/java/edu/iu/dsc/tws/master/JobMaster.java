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
import edu.iu.dsc.tws.rsched.core.SchedulerContext;

public class JobMaster extends Thread {
  private static final Logger LOG = Logger.getLogger(JobMaster.class.getName());

  public static final int JOB_MASTER_ID = -1;
  private static Progress looper;

  private Config config;
  private String masterAddress;
  private int masterPort;

  private RRServer rrServer;
  private WorkerMonitor workerMonitor;
  private int numberOfWorkers;
  private boolean workersCompleted = false;

  public JobMaster(Config config, String masterAddress) {
    this.config = config;
    this.masterAddress = masterAddress;
    this.masterPort = JobMasterContext.jobMasterPort(config);
    this.numberOfWorkers = SchedulerContext.workerInstances(config);
  }

  public void init() {
    looper = new Progress();

    ServerConnectHandler connectHandler = new ServerConnectHandler();
    rrServer =
        new RRServer(config, masterAddress, masterPort, looper, JOB_MASTER_ID, connectHandler);

    workerMonitor = new WorkerMonitor(this, rrServer, numberOfWorkers);

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

    int loopCount = 10;
    int sleepDuration = 100;

    // initially wait on a blocking call until the first client connects
    looper.loopBlocking();

    while (!workersCompleted) {

      outerLoop:
      for (int i = 0; i < loopCount; i++) {
        if (workersCompleted) {
          break outerLoop;
        }
        looper.loop();
      }

      try {
        sleep(sleepDuration);
      } catch (InterruptedException e) {
        if (!workersCompleted) {
          LOG.info("Sleep interrupted. ");
        }
      }
    }

//    for (int i = 0; i < loopCount; i++) {
//      looper.loop();
//    }
    rrServer.stop();
  }

  private void looop(int loopCount) {
    for (int i = 0; i < loopCount; i++) {
      looper.loop();
    }
  }

  /**
   * this method is executed when worker completed messages received from all workers
   */
  public void allWorkersCompleted() {

    LOG.info("All workers have completed.");
    workersCompleted = true;
    interrupt();
  }

  public class ServerConnectHandler implements ConnectHandler {
    @Override
    public void onError(SocketChannel channel) {
    }

    @Override
    public void onConnect(SocketChannel channel, StatusCode status) {
      try {
        LOG.finer("Client connected from:" + channel.getRemoteAddress());
      } catch (IOException e) {
        e.printStackTrace();
      }
    }

    @Override
    public void onClose(SocketChannel channel) {
    }
  }

  public static void main(String[] args) {

    Logger.getLogger("edu.iu.dsc.tws.common.net.tcp").setLevel(Level.WARNING);

    String host = "localhost";
    int port = 11111;
    int numberOfWorkers = 1;

    if (args.length == 1) {
      numberOfWorkers = Integer.parseInt(args[0]);
    }

    Config cfg = Config.newBuilder()
        .put(SchedulerContext.TWISTER2_WORKER_INSTANCES, numberOfWorkers)
        .put(JobMasterContext.JOB_MASTER_PORT, port)
        .build();

    JobMaster jobMaster = new JobMaster(cfg, host);
    jobMaster.init();

    LOG.info("JobMaster started on port " + port + " with " + numberOfWorkers
        + "\nWill run until all workers complete");
  }

}
