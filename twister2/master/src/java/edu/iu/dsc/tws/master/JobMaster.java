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

import com.google.protobuf.Message;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.net.tcp.Progress;
import edu.iu.dsc.tws.common.net.tcp.StatusCode;
import edu.iu.dsc.tws.common.net.tcp.request.ConnectHandler;
import edu.iu.dsc.tws.common.net.tcp.request.MessageHandler;
import edu.iu.dsc.tws.common.net.tcp.request.RRServer;
import edu.iu.dsc.tws.common.net.tcp.request.RequestID;
import edu.iu.dsc.tws.proto.network.Network;
import edu.iu.dsc.tws.proto.network.Network.ListWorkersRequest;
import edu.iu.dsc.tws.proto.network.Network.ListWorkersResponse;

public class JobMaster {
  private static final Logger LOG = Logger.getLogger(JobMaster.class.getName());

  public static final int JOB_MASTER_ID = -1;
//  private static ExecutorService threadsPool;
  private static Progress looper;

  private Config config;
  private String masterAddress;
  private int masterPort;

  private RRServer rrServer;
  private WorkerMonitor workerMonitor;
  private int numberOfWorkers;

  public JobMaster(Config config, String masterAddress, int masterPort, int numberOfWorkers) {
    this.config = config;
    this.masterAddress = masterAddress;
    this.masterPort = masterPort;
    this.numberOfWorkers = numberOfWorkers;
  }

  public void init() {
//    threadsPool = Executors.newSingleThreadExecutor();
    looper = new Progress();

    ServerConnectHandler connectHandler = new ServerConnectHandler();
    rrServer =
        new RRServer(config, masterAddress, masterPort, looper, JOB_MASTER_ID, connectHandler);

    workerMonitor = new WorkerMonitor(rrServer, numberOfWorkers);

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
  }

  public void close() {
//    threadsPool.shutdownNow();
  }

  class RequestMessageHandler implements MessageHandler {

    @Override
    public void onMessage(RequestID id, int workerId, Message message) {

      if (message instanceof Network.Ping) {
        LOG.info("Ping message received from the worker: " + workerId + "\n" + message);

      } else if (message instanceof Network.WorkerStateChange) {
        LOG.info("WorkerStateChange message received from the worker: " + workerId
            + "\n" + message);

        Network.WorkerStateChangeResponse response = Network.WorkerStateChangeResponse.newBuilder()
            .setWorkerID(workerId)
            .build();

        rrServer.sendResponse(id, response);
        System.out.println("WorkerStateChangeResponse sent to the worker: " + workerId
            + "\n" + response);
      }
    }
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

    Logger.getLogger("edu.iu.dsc.tws.common.net.tcp").setLevel(Level.SEVERE);

    Config config = null;
    String host = "localhost";
    int port = 11111;
    int numberOfWorkers = 1;

    if (args.length == 1) {
      numberOfWorkers = Integer.parseInt(args[0]);
    }

    JobMaster jobMaster = new JobMaster(config, host, port, numberOfWorkers);
    jobMaster.init();

    long timeToRun = 100; // in seconds
    LOG.info("JobMaster started on port " + port + " will run " + timeToRun + " seconds");

    timeToRun *= 1000;
    long start = System.currentTimeMillis();
    long duration = 0;
    int printIntervals = 100; // in ms
    int printCount = -1;

    while (duration < timeToRun) {
      looper.loopBlocking();

      duration = System.currentTimeMillis() - start;

      if (duration / printIntervals > printCount) {
        printCount = (int) (duration / printIntervals);
        System.out.println(printCount + ": looping");
      }
//      looper.loop();
    }

    jobMaster.close();
  }

}
