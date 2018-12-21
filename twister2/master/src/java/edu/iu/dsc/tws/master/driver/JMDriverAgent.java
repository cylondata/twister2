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
package edu.iu.dsc.tws.master.driver;

import java.nio.channels.SocketChannel;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.driver.IDriver;
import edu.iu.dsc.tws.common.driver.WorkerListener;
import edu.iu.dsc.tws.common.net.tcp.Progress;
import edu.iu.dsc.tws.common.net.tcp.StatusCode;
import edu.iu.dsc.tws.common.net.tcp.request.BlockingSendException;
import edu.iu.dsc.tws.common.net.tcp.request.ConnectHandler;
import edu.iu.dsc.tws.common.net.tcp.request.MessageHandler;
import edu.iu.dsc.tws.common.net.tcp.request.RRClient;
import edu.iu.dsc.tws.common.net.tcp.request.RRServer;
import edu.iu.dsc.tws.common.net.tcp.request.RequestID;
import edu.iu.dsc.tws.master.JobMasterContext;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;

public final class JMDriverAgent {
  private static final Logger LOG = Logger.getLogger(JMDriverAgent.class.getName());

  private static Progress looper;
  private boolean stopLooper = false;

  private Config config;

  private String masterAddress;
  private int masterPort;

  /**
   * an upto date value of number of workers in the job
   * when new workers are added/removed, we update it
   * we use this value, when we send messages to JobMaster
   */
  private int numberOfWorkers;

  private RRClient rrClient;

  /**
   * the driver can register a workerListener to get messages sent by workers
   */
  private WorkerListener workerListener;

  /**
   * the driver can register a workerListener to get messages sent by workers
   */
  private IDriver driver;

  /**
   * a singleton object for this class
   */
  private static JMDriverAgent driverAgent;

  /**
   * the maximum duration this client will try to connect to the Job Master
   * in milli seconds
   */
  private static final long CONNECTION_TRY_TIME_LIMIT = 100000;

  /**
   * to control the connection error when we repeatedly try connecting
   */
  private boolean connectionRefused = false;

  /**
   * broadcast response message
   * it is used to transfer from message listener to sending method
   */
  private JobMasterAPI.BroadcastResponse broadcastResponse;

  /**
   * The Agent class that talks the driver with the JobMaster
   */
  private JMDriverAgent(Config config,
                        String masterHost,
                        int masterPort,
                        int numberOfWorkers) {
    this.config = config;
    this.masterAddress = masterHost;
    this.masterPort = masterPort;
    this.numberOfWorkers = numberOfWorkers;
  }

  public static JMDriverAgent createJMDriverAgent(Config config,
                                                  String masterHost,
                                                  int masterPort,
                                                  int numberOfWorkers) {

    if (driverAgent != null) {
      return driverAgent;
    }

    driverAgent = new JMDriverAgent(config, masterHost, masterPort, numberOfWorkers);
    return driverAgent;
  }

  /**
   * initialize JMDriverAgent
   * wait until it connects to JobMaster
   * return false, if it can not connect to JobMaster
   */
  private void init() {

    looper = new Progress();

    ClientConnectHandler connectHandler = new ClientConnectHandler();

    rrClient = new RRClient(masterAddress, masterPort, config, looper,
        RRServer.DRIVER_ID, connectHandler);

    // protocol buffer message registrations
    JobMasterAPI.RegisterDriver.Builder registerBuilder =
        JobMasterAPI.RegisterDriver.newBuilder();
    JobMasterAPI.RegisterDriverResponse.Builder registerResponseBuilder
        = JobMasterAPI.RegisterDriverResponse.newBuilder();

    JobMasterAPI.WorkersScaled.Builder scaledMessageBuilder =
        JobMasterAPI.WorkersScaled.newBuilder();
    JobMasterAPI.ScaledResponse.Builder scaledResponseBuilder
        = JobMasterAPI.ScaledResponse.newBuilder();

    JobMasterAPI.Broadcast.Builder broadcastBuilder = JobMasterAPI.Broadcast.newBuilder();
    JobMasterAPI.BroadcastResponse.Builder broadcastResponseBuilder
        = JobMasterAPI.BroadcastResponse.newBuilder();

    JobMasterAPI.WorkerToDriver.Builder toDriverBuilder = JobMasterAPI.WorkerToDriver.newBuilder();

    JobMasterAPI.WorkersJoined.Builder joinedBuilder = JobMasterAPI.WorkersJoined.newBuilder();

    ResponseMessageHandler responseMessageHandler = new ResponseMessageHandler();
    rrClient.registerResponseHandler(registerBuilder, responseMessageHandler);
    rrClient.registerResponseHandler(registerResponseBuilder, responseMessageHandler);

    rrClient.registerResponseHandler(scaledMessageBuilder, responseMessageHandler);
    rrClient.registerResponseHandler(scaledResponseBuilder, responseMessageHandler);

    rrClient.registerResponseHandler(broadcastBuilder, responseMessageHandler);
    rrClient.registerResponseHandler(broadcastResponseBuilder, responseMessageHandler);

    rrClient.registerResponseHandler(toDriverBuilder, responseMessageHandler);

    rrClient.registerResponseHandler(joinedBuilder, responseMessageHandler);

    // try to connect to JobMaster
    tryUntilConnected(CONNECTION_TRY_TIME_LIMIT);

    if (!rrClient.isConnected()) {
      throw new RuntimeException("JMWorkerAgent can not connect to Job Master. Exiting .....");
    }
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
   * stop the JMWorkerAgent
   */
  public void close() {
    stopLooper = true;
    looper.wakeup();
  }

  private void startLooping() {

    while (!stopLooper) {
      looper.loopBlocking();
    }

    rrClient.disconnect();
  }

  /**
   * start the Job Master Client in a Thread
   */
  public Thread startThreaded() {
    // first initialize the client, connect to Job Master
    init();

    Thread jmThread = new Thread() {
      public void run() {
        startLooping();
      }
    };

    jmThread.start();

    // register the driver
    registerDriver();

    return jmThread;
  }

  /**
   * start the Job Master Client in a blocking call
   */
  public void startBlocking() {
    // first initialize the client, connect to Job Master
    init();

    startLooping();

    // register the driver
    registerDriver();
  }

  public void setNumberOfWorkers(int numberOfWorkers) {
    this.numberOfWorkers = numberOfWorkers;
  }

  /**
   * only one WorkerListener can be added
   * if the second WorkerListener tried to be added, false returned
   */
  public static boolean addWorkerListener(WorkerListener workerListener) {
    if (driverAgent.workerListener != null) {
      return false;
    }

    driverAgent.workerListener = workerListener;
    return true;
  }

  /**
   * only one IDriver can be added
   * if the second IDriver tried to be added, false returned
   */
  public boolean addIDriver(IDriver iDriver) {

    if (driver != null) {
      return false;
    }

    driver = iDriver;
    return true;
  }

  /**
   * send RegisterDriver message to Job Master
   */
  private void registerDriver() {

    JobMasterAPI.RegisterDriver registerDriver = JobMasterAPI.RegisterDriver.newBuilder().build();

    LOG.fine("Sending RegisterDriver message: \n" + registerDriver);
    rrClient.sendRequest(registerDriver);
  }

  /**
   * send scaled message
   */
  public boolean sendScaledMessage(int change, int numOfWorkers) {

    JobMasterAPI.WorkersScaled scaledMessage =
        JobMasterAPI.WorkersScaled.newBuilder()
            .setChange(change)
            .setNumberOfWorkers(numOfWorkers)
            .build();

    LOG.info("Sending WorkersScaled message: \n" + scaledMessage);

    // wait for the response
    try {
      rrClient.sendRequestWaitResponse(scaledMessage,
          JobMasterContext.responseWaitDuration(config));

      return true;

    } catch (BlockingSendException bse) {
      LOG.log(Level.SEVERE, bse.getMessage(), bse);
      return false;
    }
  }

  /**
   * broadcast a message to all workers in the job
   */
  public boolean sendBroadcastMessage(Message message) {
    JobMasterAPI.Broadcast broadcast = JobMasterAPI.Broadcast.newBuilder()
        .setData(Any.pack(message).toByteString())
        .setNumberOfWorkers(numberOfWorkers)
        .build();

    LOG.fine("Sending Broadcast message: \n" + broadcast);

    // wait for the response
    try {
      rrClient.sendRequestWaitResponse(broadcast, JobMasterContext.responseWaitDuration(config));
      if (broadcastResponse != null && broadcastResponse.getSucceeded()) {
        broadcastResponse = null;
        return true;
      } else {
        broadcastResponse = null;
        return false;
      }

    } catch (BlockingSendException bse) {
      LOG.log(Level.SEVERE, bse.getMessage(), bse);
      return false;
    }
  }

  private class ResponseMessageHandler implements MessageHandler {

    @Override
    public void onMessage(RequestID id, int workerId, Message message) {

      if (message instanceof JobMasterAPI.RegisterDriverResponse) {

        JobMasterAPI.RegisterDriverResponse regResponse =
            (JobMasterAPI.RegisterDriverResponse) message;

        if (regResponse.getSucceeded()) {
          LOG.fine("Registering the driver successful. ");
        } else {
          LOG.severe("Registering the driver unsuccessful. Reason: " + regResponse.getReason());
        }

      } else if (message instanceof JobMasterAPI.ScaledResponse) {

        JobMasterAPI.ScaledResponse scaledResponse = (JobMasterAPI.ScaledResponse) message;

        if (scaledResponse.getSucceeded()) {
          LOG.fine("JobMaster processed WorkersScaled message successfully.");
        } else {
          LOG.severe("JobMaster could not process WorkersScaled message successfully. Reason: "
              + scaledResponse.getReason());
        }

      } else if (message instanceof JobMasterAPI.BroadcastResponse) {

        broadcastResponse = (JobMasterAPI.BroadcastResponse) message;
        if (broadcastResponse.getSucceeded()) {
          LOG.info("Broadcasting the message is successful.");
        } else {
          LOG.severe("Broadcasting the message is unsuccessful. Reason: "
              + broadcastResponse.getReason());
        }

      } else if (message instanceof JobMasterAPI.WorkerToDriver) {
        LOG.fine("Received WorkerToDriver message from a worker. \n" + message);

        if (workerListener != null) {
          JobMasterAPI.WorkerToDriver toDriver = (JobMasterAPI.WorkerToDriver) message;
          try {
            Any any = Any.parseFrom(toDriver.getData());
            workerListener.workerMessageReceived(any, toDriver.getWorkerID());
          } catch (InvalidProtocolBufferException e) {
            LOG.log(Level.SEVERE, "Can not parse received protocol buffer message to Any", e);
          }
        }

      } else if (message instanceof JobMasterAPI.WorkersJoined) {
        LOG.fine("Received WorkersJoined message from JobMaster. \n" + message);

        if (driver != null) {
          JobMasterAPI.WorkersJoined joinedMessage = (JobMasterAPI.WorkersJoined) message;
          driver.allWorkersJoined(joinedMessage.getWorkerList());
        }

      } else {

        LOG.warning("Received message unrecognized. \n" + message);

      }
    }
  }

  private class ClientConnectHandler implements ConnectHandler {
    @Override
    public void onError(SocketChannel channel) {

    }

    @Override
    public void onConnect(SocketChannel channel, StatusCode status) {
      if (status == StatusCode.SUCCESS) {
        LOG.info("JMDriverAgent connected to JobMaster: " + channel);
      }

      if (status == StatusCode.CONNECTION_REFUSED) {
        connectionRefused = true;
      }
    }

    @Override
    public void onClose(SocketChannel channel) {

    }
  }


}
