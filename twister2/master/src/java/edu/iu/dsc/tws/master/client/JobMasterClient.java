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

package edu.iu.dsc.tws.master.client;

import java.nio.channels.SocketChannel;
import java.util.logging.Logger;

import com.google.protobuf.Message;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.discovery.WorkerNetworkInfo;
import edu.iu.dsc.tws.common.net.tcp.Progress;
import edu.iu.dsc.tws.common.net.tcp.StatusCode;
import edu.iu.dsc.tws.common.net.tcp.request.ConnectHandler;
import edu.iu.dsc.tws.common.net.tcp.request.MessageHandler;
import edu.iu.dsc.tws.common.net.tcp.request.RRClient;
import edu.iu.dsc.tws.common.net.tcp.request.RequestID;
import edu.iu.dsc.tws.master.JobMasterContext;
import edu.iu.dsc.tws.proto.network.Network;
import edu.iu.dsc.tws.proto.network.Network.ListWorkersRequest;
import edu.iu.dsc.tws.proto.network.Network.ListWorkersResponse;

public class JobMasterClient extends Thread {
  private static final Logger LOG = Logger.getLogger(JobMasterClient.class.getName());

  private static Progress looper;
  private boolean stopLooper = false;

  private Config config;
  private WorkerNetworkInfo thisWorker;

  private String masterAddress;
  private int masterPort;

  private RRClient rrClient;
  private Pinger pinger;
  private WorkerDiscoverer workerController;

  private boolean startingMessageSent = false;

  private int numberOfWorkers;

  public JobMasterClient(Config config, WorkerNetworkInfo thisWorker) {
    this(config, thisWorker, JobMasterContext.jobMasterIP(config),
        JobMasterContext.jobMasterPort(config), JobMasterContext.workerInstances(config));
  }

  public JobMasterClient(Config config, WorkerNetworkInfo thisWorker,
                         String masterHost, int masterPort, int numberOfWorkers) {
    this.config = config;
    this.thisWorker = thisWorker;
    this.masterAddress = masterHost;
    this.masterPort = masterPort;
    this.numberOfWorkers = numberOfWorkers;
  }

  public JobMasterClient(Config config, WorkerNetworkInfo thisWorker, String jobMasterIP) {
    this.config = config;
    this.thisWorker = thisWorker;
    this.masterAddress = jobMasterIP;
    this.masterPort = JobMasterContext.jobMasterPort(config);
  }


  /**
   * initialize JobMasterClient
   * wait until it connects to JobMaster
   * return false, if it can not connect to JobMaster
   * @return
   */
  public boolean init() {

    looper = new Progress();

    ClientConnectHandler connectHandler = new ClientConnectHandler();
    rrClient = new RRClient(masterAddress, masterPort, null, looper,
        thisWorker.getWorkerID(), connectHandler);

    long interval = JobMasterContext.pingInterval(config);
    pinger = new Pinger(thisWorker, rrClient, interval);

    workerController = new WorkerDiscoverer(config, thisWorker, rrClient, numberOfWorkers);

    Network.Ping.Builder pingBuilder = Network.Ping.newBuilder();
    rrClient.registerResponseHandler(pingBuilder, pinger);

    ListWorkersRequest.Builder listRequestBuilder = ListWorkersRequest.newBuilder();
    ListWorkersResponse.Builder listResponseBuilder = ListWorkersResponse.newBuilder();
    rrClient.registerResponseHandler(listRequestBuilder, workerController);
    rrClient.registerResponseHandler(listResponseBuilder, workerController);

    Network.WorkerStateChange.Builder stateChangeBuilder = Network.WorkerStateChange.newBuilder();
    Network.WorkerStateChangeResponse.Builder stateChangeResponseBuilder
        = Network.WorkerStateChangeResponse.newBuilder();

    ResponseMessageHandler responseMessageHandler = new ResponseMessageHandler();
    rrClient.registerResponseHandler(stateChangeBuilder, responseMessageHandler);
    rrClient.registerResponseHandler(stateChangeResponseBuilder, responseMessageHandler);

    // try to connect to JobMaster, wait up to 100 seconds
    // make this one config value
    long connectionTimeLimit = 100000;
    tryUntilConnected(connectionTimeLimit);

    if (rrClient.isConnected()) {
      LOG.info("JobMasterClient connected to JobMaster.");
    } else {
      LOG.severe("JobMasterClient can not connect to Job Master. Exiting .....");
      return false;
    }

    this.start();
    return true;
  }

  public WorkerDiscoverer getWorkerController() {
    return workerController;
  }

  public void close() {
    stopLooper = true;
    looper.wakeup();
  }

  @Override
  public void run() {

    while (!stopLooper) {
      long timeToNextPing = pinger.timeToNextPing();
      if (timeToNextPing < 30 && startingMessageSent) {
        pinger.sendPingMessage();
      } else {
        looper.loopBlocking(timeToNextPing);
      }
    }

    rrClient.disconnect();
  }

  public boolean tryUntilConnected(long timeLimit) {
    long startTime = System.currentTimeMillis();
    long duration = 0;
    long sleepInterval = 30;

    long logInterval = 1000;
    long nextLogTime = logInterval;

    while (duration < timeLimit) {
      // try connecting
      rrClient.connect();
      // loop once to connect
      looper.loop();

      if (rrClient.isConnected()) {
        return true;
      }

      try {
        sleep(sleepInterval);
      } catch (InterruptedException e) {
        LOG.warning("Sleep interrupted.");
      }

      if (rrClient.isConnected()) {
        return true;
      }

      duration = System.currentTimeMillis() - startTime;

      if (duration > nextLogTime) {
        LOG.info("Still trying to connect to Job Master");
        nextLogTime += logInterval;
      }
    }

    return false;
  }

  public WorkerNetworkInfo sendWorkerStartingMessage() {
    Network.WorkerStateChange workerStateChange = Network.WorkerStateChange.newBuilder()
        .setWorkerID(thisWorker.getWorkerID())
        .setNewState(Network.WorkerState.STARTING)
        .setWorkerIP(thisWorker.getWorkerIP().getHostAddress())
        .setWorkerPort(thisWorker.getWorkerPort())
        .build();

    LOG.info("Sending the Worker Starting message: \n" + workerStateChange);

    RequestID requestID = null;
    if (JobMasterContext.jobMasterAssignsWorkerIDs(config)) {
      requestID = rrClient.sendRequestWaitResponse(workerStateChange,
          JobMasterContext.responseWaitDuration(config));
    } else {
      requestID = rrClient.sendRequest(workerStateChange);
    }

    if (requestID == null) {
      LOG.severe("Couldn't send Worker Starting message or couldn't receive the response on time.");
      return null;
    }

    startingMessageSent = true;
    pinger.sendPingMessage();

    return thisWorker;
  }

  public boolean sendWorkerRunningMessage() {
    Network.WorkerStateChange workerStateChange = Network.WorkerStateChange.newBuilder()
        .setWorkerID(thisWorker.getWorkerID())
        .setNewState(Network.WorkerState.RUNNING)
        .build();

    RequestID requestID = rrClient.sendRequest(workerStateChange);
    if (requestID == null) {
      LOG.severe("Could not send Worker Running message.");
      return false;
    }

    LOG.info("Sent the Worker Running message: \n" + workerStateChange);
    return true;
  }

  public boolean sendWorkerCompletedMessage() {

    Network.WorkerStateChange workerStateChange = Network.WorkerStateChange.newBuilder()
        .setWorkerID(thisWorker.getWorkerID())
        .setNewState(Network.WorkerState.COMPLETED)
        .build();

    LOG.info("Sending the Worker Completed message: \n" + workerStateChange);
    RequestID requestID = rrClient.sendRequestWaitResponse(workerStateChange,
        JobMasterContext.responseWaitDuration(config));

    if (requestID == null) {
      LOG.severe("Couldn't send Worker Completed message or couldn't receive the response.");
      return false;
    }

    return true;
  }

  class ResponseMessageHandler implements MessageHandler {

    @Override
    public void onMessage(RequestID id, int workerId, Message message) {

      if (message instanceof Network.WorkerStateChangeResponse) {
        LOG.info("Received a WorkerStateChange response from the master. \n" + message);

        Network.WorkerStateChangeResponse responseMessage =
            (Network.WorkerStateChangeResponse) message;

        if (JobMasterContext.jobMasterAssignsWorkerIDs(config)
            && responseMessage.getSentState() == Network.WorkerState.STARTING) {
          thisWorker.setWorkerID(responseMessage.getWorkerID());
        }

      } else {
        LOG.warning("Received message unrecognized. \n" + message);
      }

    }
  }

  public class ClientConnectHandler implements ConnectHandler {
    @Override
    public void onError(SocketChannel channel) {

    }

    @Override
    public void onConnect(SocketChannel channel, StatusCode status) {
      // put the reason into some variable
      // if server is not there, should try to reconnect
    }

    @Override
    public void onClose(SocketChannel channel) {

    }
  }

}
