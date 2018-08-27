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
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.Message;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.discovery.WorkerNetworkInfo;
import edu.iu.dsc.tws.common.net.tcp.Progress;
import edu.iu.dsc.tws.common.net.tcp.StatusCode;
import edu.iu.dsc.tws.common.net.tcp.request.BlockingSendException;
import edu.iu.dsc.tws.common.net.tcp.request.ConnectHandler;
import edu.iu.dsc.tws.common.net.tcp.request.MessageHandler;
import edu.iu.dsc.tws.common.net.tcp.request.RRClient;
import edu.iu.dsc.tws.common.net.tcp.request.RequestID;
import edu.iu.dsc.tws.master.JobMasterContext;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI.ListWorkersRequest;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI.ListWorkersResponse;

/**
 * JobMasterClient class
 * It is started for each Twister2 worker
 * It handles the communication with the Job Master
 * <p>
 * It provides:
 * worker discovery
 * barrier method
 * Ping service
 * <p>
 * It can be started in two different modes:
 * Threaded and Blocking
 * <p>
 * If the user calls:
 * startThreaded()
 * It starts as a Thread and the call to this method returns
 * <p>
 * If the user calls:
 * startBlocking()
 * It uses the calling thread and this call does not return unless the close method is called
 */

public class JobMasterClient {
  private static final Logger LOG = Logger.getLogger(JobMasterClient.class.getName());

  private static Progress looper;
  private boolean stopLooper = false;

  private Config config;
  private WorkerNetworkInfo thisWorker;

  private String masterAddress;
  private int masterPort;

  private RRClient rrClient;
  private Pinger pinger;
  private JMWorkerController jmWorkerController;

  private boolean startingMessageSent = false;

  private int numberOfWorkers;

  /**
   * the maximum duration this client will try to connect to the Job Master
   * in milli seconds
   */
  private static final long CONNECTION_TRY_TIME_LIMIT = 100000;

  /**
   * to control the connection error when we repeatedly try connecting
   */
  private boolean connectionRefused = false;

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
   */
  private boolean init() {

    looper = new Progress();

    ClientConnectHandler connectHandler = new ClientConnectHandler();
    rrClient = new RRClient(masterAddress, masterPort, null, looper,
        thisWorker.getWorkerID(), connectHandler);

    long interval = JobMasterContext.pingInterval(config);
    pinger = new Pinger(thisWorker, rrClient, interval);

    jmWorkerController = new JMWorkerController(config, thisWorker, rrClient, numberOfWorkers);

    JobMasterAPI.Ping.Builder pingBuilder = JobMasterAPI.Ping.newBuilder();
    rrClient.registerResponseHandler(pingBuilder, pinger);

    ListWorkersRequest.Builder listRequestBuilder = ListWorkersRequest.newBuilder();
    ListWorkersResponse.Builder listResponseBuilder = ListWorkersResponse.newBuilder();
    rrClient.registerResponseHandler(listRequestBuilder, jmWorkerController);
    rrClient.registerResponseHandler(listResponseBuilder, jmWorkerController);

    JobMasterAPI.WorkerStateChange.Builder stateChangeBuilder =
        JobMasterAPI.WorkerStateChange.newBuilder();
    JobMasterAPI.WorkerStateChangeResponse.Builder stateChangeResponseBuilder
        = JobMasterAPI.WorkerStateChangeResponse.newBuilder();

    ResponseMessageHandler responseMessageHandler = new ResponseMessageHandler();
    rrClient.registerResponseHandler(stateChangeBuilder, responseMessageHandler);
    rrClient.registerResponseHandler(stateChangeResponseBuilder, responseMessageHandler);

    JobMasterAPI.BarrierRequest.Builder barrierRequestBuilder =
        JobMasterAPI.BarrierRequest.newBuilder();
    JobMasterAPI.BarrierResponse.Builder barrierResponseBuilder =
        JobMasterAPI.BarrierResponse.newBuilder();
    rrClient.registerResponseHandler(barrierRequestBuilder, jmWorkerController);
    rrClient.registerResponseHandler(barrierResponseBuilder, jmWorkerController);

    // try to connect to JobMaster
    tryUntilConnected(CONNECTION_TRY_TIME_LIMIT);

    if (rrClient.isConnected()) {
      LOG.info("JobMasterClient connected to JobMaster.");
    } else {
      LOG.severe("JobMasterClient can not connect to Job Master. Exiting .....");
      return false;
    }

    return true;
  }

  public JMWorkerController getJMWorkerController() {
    return jmWorkerController;
  }

  /**
   * stop the JobMasterClient
   */
  public void close() {
    stopLooper = true;
    looper.wakeup();
  }

  private void startLooping() {

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

  /**
   * start the Job Master Client in a Thread
   */
  public Thread startThreaded() {
    // first call the init method
    boolean initialized = init();
    if (!initialized) {
      return null;
    }

    Thread jmThread = new Thread() {
      public void run() {
        startLooping();
      }
    };

    jmThread.start();

    return jmThread;
  }

  /**
   * start the Job Master Client in a blocking call
   */
  public boolean startBlocking() {
    // first call the init method
    boolean initialized = init();
    if (!initialized) {
      return false;
    }

    startLooping();

    return true;
  }

  /**
   * try connecting until the time limit is reached
   */
  public boolean tryUntilConnected(long timeLimit) {
    long startTime = System.currentTimeMillis();
    long duration = 0;
    long sleepInterval = 50;

    // log interval in milliseconds
    long logInterval = 1000;
    long nextLogTime = logInterval;

    // allow the first connection attempt
    connectionRefused = true;

    while (duration < timeLimit) {
      // try connecting
      if (connectionRefused) {
        rrClient.tryConnecting();
        connectionRefused = false;
      }

      // loop to connect
      looper.loop();

      if (rrClient.isConnected()) {
        return true;
      }

      try {
        Thread.sleep(sleepInterval);
      } catch (InterruptedException e) {
        LOG.warning("Sleep interrupted.");
      }

      if (rrClient.isConnected()) {
        return true;
      }

      duration = System.currentTimeMillis() - startTime;

      if (duration > nextLogTime) {
        LOG.info("Still trying to connect to the Job Master: " + masterAddress + ":" + masterPort);
        nextLogTime += logInterval;
      }
    }

    return false;
  }

  /**
   * send worker STARTING message
   * put WorkerNetworkInfo in that message
   * @return
   */
  public WorkerNetworkInfo sendWorkerStartingMessage() {

    JobMasterAPI.WorkerNetworkInfo.Builder workerInfoBuilder =
        JobMasterAPI.WorkerNetworkInfo.newBuilder()
        .setWorkerID(thisWorker.getWorkerID())
        .setWorkerIP(thisWorker.getWorkerIP().getHostAddress())
        .setPort(thisWorker.getWorkerPort());

    if (thisWorker.getNodeInfo().hasNodeIP()) {
      workerInfoBuilder.setNodeIP(thisWorker.getNodeInfo().getNodeIP());
    }

    if (thisWorker.getNodeInfo().hasRackName()) {
      workerInfoBuilder.setRackName(thisWorker.getNodeInfo().getRackName());
    }

    if (thisWorker.getNodeInfo().hasDataCenterName()) {
      workerInfoBuilder.setDataCenterName(thisWorker.getNodeInfo().getDataCenterName());
    }

    JobMasterAPI.WorkerStateChange workerStateChange = JobMasterAPI.WorkerStateChange.newBuilder()
        .setWorkerNetworkInfo(workerInfoBuilder.build())
        .setNewState(JobMasterAPI.WorkerState.STARTING)
        .build();

    // if JobMaster assigns ID, wait for the response
    if (JobMasterContext.jobMasterAssignsWorkerIDs(config)) {
      LOG.info("Sending the Worker Starting message: \n" + workerStateChange);
      try {
        rrClient.sendRequestWaitResponse(workerStateChange,
            JobMasterContext.responseWaitDuration(config));

      } catch (BlockingSendException bse) {
        LOG.log(Level.SEVERE, bse.getMessage(), bse);
        return null;
      }

    } else {
      RequestID requestID = rrClient.sendRequest(workerStateChange);
      if (requestID == null) {
        LOG.severe("Couldn't send Worker Starting message: " + workerStateChange);
        return null;
      }
    }

    startingMessageSent = true;
    pinger.sendPingMessage();

    return thisWorker;
  }

  public boolean sendWorkerRunningMessage() {
    JobMasterAPI.WorkerNetworkInfo workerInfo =
        JobMasterAPI.WorkerNetworkInfo.newBuilder()
            .setWorkerID(thisWorker.getWorkerID())
            .build();

    JobMasterAPI.WorkerStateChange workerStateChange = JobMasterAPI.WorkerStateChange.newBuilder()
        .setWorkerNetworkInfo(workerInfo)
        .setNewState(JobMasterAPI.WorkerState.RUNNING)
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

    JobMasterAPI.WorkerNetworkInfo workerInfo =
        JobMasterAPI.WorkerNetworkInfo.newBuilder()
            .setWorkerID(thisWorker.getWorkerID())
            .build();

    JobMasterAPI.WorkerStateChange workerStateChange = JobMasterAPI.WorkerStateChange.newBuilder()
        .setWorkerNetworkInfo(workerInfo)
        .setNewState(JobMasterAPI.WorkerState.COMPLETED)
        .build();

    LOG.info("Sending the Worker Completed message: \n" + workerStateChange);
    try {
      rrClient.sendRequestWaitResponse(workerStateChange,
          JobMasterContext.responseWaitDuration(config));
    } catch (BlockingSendException e) {
      LOG.log(Level.SEVERE, e.getMessage(), e);
      return false;
    }

    return true;
  }

  class ResponseMessageHandler implements MessageHandler {

    @Override
    public void onMessage(RequestID id, int workerId, Message message) {

      if (message instanceof JobMasterAPI.WorkerStateChangeResponse) {
        LOG.info("Received a WorkerStateChange response from the master. \n" + message);

        JobMasterAPI.WorkerStateChangeResponse responseMessage =
            (JobMasterAPI.WorkerStateChangeResponse) message;

        if (JobMasterContext.jobMasterAssignsWorkerIDs(config)
            && responseMessage.getSentState() == JobMasterAPI.WorkerState.STARTING) {
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
      if (status == StatusCode.CONNECTION_REFUSED) {
        connectionRefused = true;
      }
    }

    @Override
    public void onClose(SocketChannel channel) {

    }
  }

}
