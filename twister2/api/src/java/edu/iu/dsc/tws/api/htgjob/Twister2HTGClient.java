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
package edu.iu.dsc.tws.api.htgjob;

import java.nio.channels.SocketChannel;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.Message;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.net.tcp.Progress;
import edu.iu.dsc.tws.common.net.tcp.StatusCode;
import edu.iu.dsc.tws.common.net.tcp.request.BlockingSendException;
import edu.iu.dsc.tws.common.net.tcp.request.ConnectHandler;
import edu.iu.dsc.tws.common.net.tcp.request.MessageHandler;
import edu.iu.dsc.tws.common.net.tcp.request.RRClient;
import edu.iu.dsc.tws.common.net.tcp.request.RequestID;
import edu.iu.dsc.tws.common.resource.WorkerInfoUtils;
import edu.iu.dsc.tws.master.JobMasterContext;
import edu.iu.dsc.tws.master.client.JMWorkerController;
import edu.iu.dsc.tws.master.client.Pinger;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.proto.system.job.HTGJobAPI;

public class Twister2HTGClient {

  private static final Logger LOG = Logger.getLogger(Twister2HTGClient.class.getName());

  private static Progress looper;
  private boolean stopLooper = false;

  private Config config;
  private JobMasterAPI.WorkerInfo thisWorker;

  private JobMasterAPI.HTGClientInfo thisClient;

  private String masterAddress;
  private int masterPort;

  private RRClient rrClient;
  private Pinger pinger;
  private JMWorkerController jmWorkerController;

  private boolean startingMessageSent = false;

  private int numberOfWorkers;

  public HTGJobAPI.HTGJob getHtgJob() {
    return htgJob;
  }

  public void setHtgJob(HTGJobAPI.HTGJob htgJob) {
    this.htgJob = htgJob;
  }

  public HTGJobAPI.ExecuteMessage getExecuteMessage() {
    return executeMessage;
  }

  public void setExecuteMessage(HTGJobAPI.ExecuteMessage executeMessage) {
    this.executeMessage = executeMessage;
  }

  //NEWLY Added
  private HTGJobAPI.HTGJob htgJob;
  private HTGJobAPI.ExecuteMessage executeMessage;

  /**
   * the maximum duration this client will try to connect to the Job Master
   * in milli seconds
   */
  private static final long CONNECTION_TRY_TIME_LIMIT = 100000;

  /**
   * to control the connection error when we repeatedly try connecting
   */
  private boolean connectionRefused = false;

  public Twister2HTGClient(Config config,
                           JobMasterAPI.HTGClientInfo thisClient,
                           HTGJobAPI.HTGJob htgJob,
                           HTGJobAPI.ExecuteMessage executeMessage) {
    this(config, thisClient, JobMasterContext.jobMasterIP(config),
        JobMasterContext.jobMasterPort(config), htgJob, executeMessage);
  }

  public Twister2HTGClient(Config config,
                           JobMasterAPI.HTGClientInfo thisClient,
                           String masterHost,
                           int masterPort,
                           HTGJobAPI.HTGJob htgjob,
                           HTGJobAPI.ExecuteMessage message) {
    this.config = config;
    this.thisClient = thisClient;
    this.masterAddress = masterHost;
    this.masterPort = masterPort;

    //newly added
    this.htgJob = htgjob;
    this.executeMessage = message;
  }

  private boolean init() {

    looper = new Progress();

    Twister2HTGClient.ClientConnectHandler connectHandler
        = new Twister2HTGClient.ClientConnectHandler();

    rrClient = new RRClient(masterAddress, masterPort, null, looper,
        thisClient.getClientID(), connectHandler);

    //jmWorkerController = new JMWorkerController(config, thisClient, rrClient, numberOfWorkers);

    JobMasterAPI.HTGJobRequest.Builder htgjobRequestBuilder
        = JobMasterAPI.HTGJobRequest.newBuilder();
    JobMasterAPI.HTGJobResponse.Builder htgjobResponseBUilder
        = JobMasterAPI.HTGJobResponse.newBuilder();

    Twister2HTGClient.ResponseMessageHandler responseMessageHandler
        = new Twister2HTGClient.ResponseMessageHandler();

    rrClient.registerResponseHandler(htgjobRequestBuilder, responseMessageHandler);
    rrClient.registerResponseHandler(htgjobResponseBUilder, responseMessageHandler);

    // try to connect to JobMaster
    tryUntilConnected(CONNECTION_TRY_TIME_LIMIT);

    if (!rrClient.isConnected()) {
      LOG.severe("JobMasterClient can not connect to Job Master. Exiting .....");
      return false;
    }

    return true;
  }

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
        //startLooping();
      }
    };

    jmThread.start();

    return jmThread;
  }

  /**
   * Try until connect the job master
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
        LOG.info("Still trying to connect to the Job Master: "
            + masterAddress + ":" + masterPort);
        nextLogTime += logInterval;
      }
    }

    return false;
  }

  class ResponseMessageHandler implements MessageHandler {

    @Override
    public void onMessage(RequestID id, int clientId, Message message) {

      LOG.info("message received (for htg):" + message);

      if (message instanceof JobMasterAPI.WorkerStateChangeResponse) {
        LOG.info("Received a WorkerStateChange response from the master. \n" + message);

        JobMasterAPI.WorkerStateChangeResponse responseMessage =
            (JobMasterAPI.WorkerStateChangeResponse) message;

        if (JobMasterContext.jobMasterAssignsWorkerIDs(config)
            && responseMessage.getSentState() == JobMasterAPI.WorkerState.STARTING) {
          thisWorker = WorkerInfoUtils.updateWorkerID(thisWorker, responseMessage.getWorkerID());
        }

      } else {
        LOG.info("Received HTG Message. \n" + message);
      }

    }
  }

  public class ClientConnectHandler implements ConnectHandler {
    @Override
    public void onError(SocketChannel channel) {
    }

    @Override
    public void onConnect(SocketChannel channel, StatusCode status) {
      if (status == StatusCode.SUCCESS) {
        LOG.info(thisClient.getClientID()
            + "JobMasterClient connected to JobMaster: " + channel);
      }

      if (status == StatusCode.CONNECTION_REFUSED) {
        connectionRefused = true;
      }
    }

    @Override
    public void onClose(SocketChannel channel) {
    }
  }

  public JMWorkerController getJMWorkerController() {
    return jmWorkerController;
  }

  /**
   * This method sends the htg job object to the job master.
   */
  public boolean sendHTGClientStartingMessage() {

    JobMasterAPI.HTGJobRequest htgJobRequest = JobMasterAPI.HTGJobRequest.newBuilder()
        .setHtgJob(htgJob)
        .setExecuteMessage(executeMessage)
        .build();

    LOG.info("HTG Job Client Message and Execute Message:" + htgJob + "\t" + executeMessage);

    if (JobMasterContext.jobMasterAssignsWorkerIDs(config)) {
      try {
        rrClient.sendRequestWaitResponse(htgJobRequest,
            JobMasterContext.responseWaitDuration(config));
      } catch (BlockingSendException bse) {
        LOG.log(Level.SEVERE, bse.getMessage(), bse);
        return false;
      }

    } else {
      RequestID requestID = rrClient.sendRequest(htgJobRequest);
      LOG.info("%%%%%% I am entering else loop in htg client starting message");
      try {
        rrClient.sendRequestWaitResponse(htgJobRequest,
            JobMasterContext.responseWaitDuration(config));
      } catch (BlockingSendException e) {
        e.printStackTrace();
      }

      if (requestID == null) {
        LOG.severe("Couldn't send HTG Client starting message: " + htgJobRequest);
        return false;
      }
    }

    startingMessageSent = true;
    //pinger.sendPingMessage();
    return true;
  }
}
