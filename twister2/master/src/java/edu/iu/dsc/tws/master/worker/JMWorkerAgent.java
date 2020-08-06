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

package edu.iu.dsc.tws.master.worker;

import java.nio.channels.CancelledKeyException;
import java.nio.channels.SocketChannel;
import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.Message;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.exceptions.net.BlockingSendException;
import edu.iu.dsc.tws.api.net.StatusCode;
import edu.iu.dsc.tws.api.net.request.ConnectHandler;
import edu.iu.dsc.tws.api.net.request.MessageHandler;
import edu.iu.dsc.tws.api.net.request.RequestID;
import edu.iu.dsc.tws.api.resource.IAllJoinedListener;
import edu.iu.dsc.tws.api.resource.IReceiverFromDriver;
import edu.iu.dsc.tws.api.resource.IScalerListener;
import edu.iu.dsc.tws.api.resource.IWorkerFailureListener;
import edu.iu.dsc.tws.checkpointing.client.CheckpointingClientImpl;
import edu.iu.dsc.tws.common.net.tcp.Progress;
import edu.iu.dsc.tws.common.net.tcp.request.RRClient;
import edu.iu.dsc.tws.master.JobMasterContext;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI.WorkerInfo;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI.WorkerState;

/**
 * JMWorkerAgent class
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

public final class JMWorkerAgent {
  private static final Logger LOG = Logger.getLogger(JMWorkerAgent.class.getName());

  private static Progress looper;
  private boolean stopLooper = false;

  private Config config;
  private WorkerInfo thisWorker;

  private String jmAddress;
  private int jmPort;

  private RRClient rrClient;

  private JMWorkerController workerController;
  private JMDriverAgent driverAgent;
  private JMWorkerStatusUpdater statusUpdater;

  private boolean registrationSucceeded;
  private boolean disconnected = false;

  private boolean reconnect = false;
  private boolean reconnected = false;

  private int numberOfWorkers;

  /**
   * workers can register an IScalerListener to receive events from the driver actions
   */
  private IScalerListener scalerListener;

  /**
   * if events arrive before IScalerListener added,
   * buffer those events in this list and deliver them when an IScalerListener added
   */
  private LinkedList<JobMasterAPI.JobScaled> scaledEventBuffer = new LinkedList<>();


  /**
   * workers can register an IAllJoinedListener to be informed when all workers joined
   */
  private IAllJoinedListener allJoinedListener;

  /**
   * if events arrive before IScalerListener added,
   * buffer those events in this list and deliver them when an IScalerListener added
   */
  private LinkedList<JobMasterAPI.AllJoined> allJoinedEventBuffer = new LinkedList<>();

  /**
   * This is a singleton class
   */
  private static JMWorkerAgent workerAgent;

  /**
   * the maximum duration this client will try to connect to the Job Master
   * in milli seconds
   */
  private static final long CONNECTION_TRY_TIME_LIMIT = 100000;

  private CheckpointingClientImpl checkpointClient;

  private int restartCount;
  private JobMasterAPI.WorkerState initialState;

  /**
   * Singleton JMWorkerAgent
   */
  private JMWorkerAgent(Config config,
                        WorkerInfo thisWorker,
                        String jmAddress,
                        int jmPort,
                        int numberOfWorkers,
                        int restartCount) {
    this.config = config;
    this.thisWorker = thisWorker;
    this.jmAddress = jmAddress;
    this.jmPort = jmPort;
    this.numberOfWorkers = numberOfWorkers;
    this.restartCount = restartCount;
    this.initialState = restartCount > 0 ? WorkerState.RESTARTED : WorkerState.STARTED;
  }

  /**
   * create the singleton JMWorkerAgent
   * if it is already created, return the previous one.
   */
  public static JMWorkerAgent createJMWorkerAgent(Config config,
                                                  WorkerInfo thisWorker,
                                                  String jmAddress,
                                                  int jmPort,
                                                  int numberOfWorkers,
                                                  int restartCount) {
    if (workerAgent != null) {
      return workerAgent;
    }

    workerAgent = new JMWorkerAgent(
        config, thisWorker, jmAddress, jmPort, numberOfWorkers, restartCount);
    return workerAgent;
  }

  /**
   * return the singleton agent object
   */
  public static JMWorkerAgent getJMWorkerAgent() {
    return workerAgent;
  }

  /**
   * initialize JMWorkerAgent
   * wait until it connects to JobMaster
   * return false, if it can not connect to JobMaster
   */
  private void init() {

    looper = new Progress();
    ClientConnectHandler connectHandler = new ClientConnectHandler();

    rrClient = new RRClient(jmAddress, jmPort, null, looper,
        thisWorker.getWorkerID(), connectHandler);

    driverAgent = new JMDriverAgent(rrClient, thisWorker.getWorkerID());
    statusUpdater = new JMWorkerStatusUpdater(rrClient, thisWorker.getWorkerID(), config);

    // protocol buffer message registrations
    ResponseMessageHandler handler = new ResponseMessageHandler();
    rrClient.registerResponseHandler(JobMasterAPI.RegisterWorker.newBuilder(), handler);
    rrClient.registerResponseHandler(JobMasterAPI.RegisterWorkerResponse.newBuilder(), handler);

    rrClient.registerResponseHandler(JobMasterAPI.JobScaled.newBuilder(), handler);
    rrClient.registerResponseHandler(JobMasterAPI.AllJoined.newBuilder(), handler);

    // create checkpointing client
    this.checkpointClient = new CheckpointingClientImpl(rrClient, this.config.getLongValue(
        CheckpointingClientImpl.CONFIG_WAIT_TIME,
        10000
    ));

    workerController = new JMWorkerController(
        config, thisWorker, numberOfWorkers, restartCount, rrClient, this.checkpointClient);

    // try to connect to JobMaster
    tryUntilConnected(CONNECTION_TRY_TIME_LIMIT);

    if (!rrClient.isConnected()) {
      throw new RuntimeException("JMWorkerAgent can not connect to Job Master. Exiting .....");
    }

    // initialize checkpointing client
    this.checkpointClient.init();
  }

  /**
   * start the client to listen for messages
   */
  private void startLooping() {

    while (!stopLooper) {
      looper.loopBlocking();

      if (reconnect) {
        // first disconnect
        LOG.fine("Worker is disconnecting from JobMaster from previous session.");
        rrClient.disconnect();

        // update jmHost and jmPort
        rrClient.setHostAndPort(jmAddress, jmPort);

        // reconnect
        reconnected = tryUntilConnected(CONNECTION_TRY_TIME_LIMIT);
        if (reconnected) {
          LOG.info("Worker " + thisWorker.getWorkerID() + " Re-connected to JobMaster.");
        } else {
          LOG.info("Worker " + thisWorker.getWorkerID() + " could not re-connect to JobMaster.");
        }

        reconnect = false;
      }
    }

    rrClient.disconnect();
  }

  /**
   * start the Job Master Client in a Thread
   */
  public Thread startThreaded() {
    // first initialize the client, connect to Job Master
    init();

    Thread jmThread = new Thread(this::startLooping);

    jmThread.setName("JM-Agent-WorkerID " + thisWorker.getWorkerID());
    jmThread.setDaemon(true);
    jmThread.start();

    boolean registered = registerWorker();

    // if there was a connection problem when registering,
    // re-try that two more times
    if (!registered && disconnected) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
      }
      registered = reconnect(jmAddress);
    }

    if (!registered && disconnected) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
      }
      registered = reconnect(jmAddress);
    }

    if (!registered) {
      this.close();
      throw new RuntimeException("Could not register Worker with JobMaster. Exiting .....");
    }

    return jmThread;
  }

  /**
   * start the Job Master Client in a blocking call
   */
  public void startBlocking() {
    // first initialize the client, connect to Job Master
    init();

    startLooping();

    //TODO: this should be tested
    boolean registered = registerWorker();
    if (!registered) {
      this.close();
      throw new RuntimeException("Could not register Worker with JobMaster. Exiting .....");
    }
  }

  /**
   * try connecting until the time limit is reached
   */
  public boolean tryUntilConnected(long timeLimit) {
    long startTime = System.currentTimeMillis();
    long duration = 0;
    long connectAttemptInterval = 100;
    long maxConnectAttemptInterval = 1000;

    // log interval in milliseconds
    long logInterval = 1000;
    long nextLogTime = logInterval;

    while (duration < timeLimit && !rrClient.isConnected()) {
      // try connecting
      rrClient.tryConnecting();

      // loop to connect
      try {
        looper.loop();
      } catch (CancelledKeyException cke) {
        LOG.severe(cke.getMessage() + " Will try to reconnect ...");
        rrClient.disconnect();
      }

      if (rrClient.isConnected()) {
        return true;
      }

      try {
        Thread.sleep(connectAttemptInterval);
      } catch (InterruptedException e) {
        LOG.warning("Sleep interrupted.");
      }

      if (connectAttemptInterval < maxConnectAttemptInterval) {
        connectAttemptInterval = Math.min(connectAttemptInterval * 2, maxConnectAttemptInterval);
      }

      duration = System.currentTimeMillis() - startTime;
      if (duration > nextLogTime) {
        LOG.info("Still trying to connect to the Job Master: " + jmAddress + ":" + jmPort);
        nextLogTime += logInterval;
      }
    }

    return false;
  }

  /**
   * this method is called after the connection is lost to job master
   * it can be called after the jm restarted
   * it reconnects the worker to the job master
   */
  public boolean reconnect(String jobMasterAddress) {

    this.jmAddress = jobMasterAddress;
    reconnect = true;
    reconnected = false;

    looper.wakeup();

    long startTime = System.currentTimeMillis();
    long delay = 0;

    while (!reconnected && delay < CONNECTION_TRY_TIME_LIMIT) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        LOG.warning("Sleep interrupted. Will try again.");
      }

      delay = System.currentTimeMillis() - startTime;
    }

    if (!reconnected) {
      throw new RuntimeException("Could not reconnect Worker with JobMaster. Exiting .....");
    }

    // register the worker
    LOG.info("Worker re-registering with JobMaster to initialize things.");
    return registerWorker();
  }

  /**
   * return WorkerInfo for this worker
   */
  public WorkerInfo getWorkerInfo() {
    return thisWorker;
  }

  /**
   * return JMWorkerController for this worker
   */
  public JMWorkerController getJMWorkerController() {
    return workerController;
  }

  /**
   * return JMSenderToDriver for this worker
   */
  public JMDriverAgent getDriverAgent() {
    return driverAgent;
  }

  public JMWorkerStatusUpdater getStatusUpdater() {
    return statusUpdater;
  }

  public CheckpointingClientImpl getCheckpointClient() {
    return checkpointClient;
  }

  /**
   * only one IScalerListener can be added
   * if the second IScalerListener tried to be added, false returned
   */
  public static boolean addScalerListener(IScalerListener scalerListener) {
    if (workerAgent.scalerListener != null) {
      return false;
    }

    workerAgent.scalerListener = scalerListener;

    // deliver buffered messages if any
    workerAgent.deliverBufferedScaledEvents();

    return true;
  }

  /**
   * only one IAllJoinedListener can be added
   * if the second IAllJoinedListener tried to be added, false returned
   */
  public static boolean addAllJoinedListener(IAllJoinedListener iAllJoinedListener) {
    if (workerAgent.allJoinedListener != null) {
      return false;
    }

    workerAgent.allJoinedListener = iAllJoinedListener;

    // deliver buffered messages if any
    workerAgent.deliverBufferedAllJoinedEvents();

    return true;
  }

  /**
   * only one IReceiverFromDriver can be added
   * if the second IReceiverFromDriver tried to be added, returns false
   */
  public static boolean addReceiverFromDriver(IReceiverFromDriver receiverFromDriver) {
    return workerAgent.getDriverAgent().addReceiverFromDriver(receiverFromDriver);
  }

  /**
   * only one IWorkerFailureListener can be added
   * if the second IWorkerFailureListener tried to be added, returns false
   */
  public static boolean addWorkerFailureListener(IWorkerFailureListener workerFailureListener) {
    return workerAgent.getStatusUpdater().addWorkerFailureListener(workerFailureListener);
  }

  /**
   * send RegisterWorker message to Job Master
   * put WorkerInfo in this message
   */
  private boolean registerWorker() {

    JobMasterAPI.RegisterWorker registerWorker = JobMasterAPI.RegisterWorker.newBuilder()
        .setWorkerID(thisWorker.getWorkerID())
        .setWorkerInfo(thisWorker)
        .setRestartCount(restartCount)
        .build();

    LOG.fine("Sending RegisterWorker message: \n" + registerWorker);

    // wait for the response
    try {
      rrClient.sendRequestWaitResponse(registerWorker,
          JobMasterContext.responseWaitDuration(config));

      if (registrationSucceeded) {
        LOG.info("Registered worker[" + thisWorker.getWorkerID() + "] with JobMaster.");
      }
      return registrationSucceeded;

    } catch (BlockingSendException bse) {
      LOG.log(Level.SEVERE, bse.getMessage(), bse);
      return false;
    }
  }

  public boolean sendWorkerCompletedMessage(JobMasterAPI.WorkerState finalState) {
    return statusUpdater.updateWorkerStatus(finalState);
  }

  /**
   * stop the JMWorkerAgent
   */
  public void close() {
    stopLooper = true;
    looper.wakeup();
  }

  /**
   * deliver all buffered messages to the IScalerListener
   */
  private void deliverBufferedScaledEvents() {

    while (!scaledEventBuffer.isEmpty()) {
      deliverToScalerListener(scaledEventBuffer.poll());
    }
  }

  /**
   * deliver all buffered messages to the IScalerListener
   */
  private void deliverBufferedAllJoinedEvents() {

    while (!allJoinedEventBuffer.isEmpty()) {
      deliverToAllJoinedListener(allJoinedEventBuffer.poll());
    }
  }

  /**
   * deliver the received event to IScalerListener
   */
  private void deliverToScalerListener(JobMasterAPI.JobScaled event) {

    if (event.getChange() > 0) {
      scalerListener.workersScaledUp(event.getChange());
    } else if (event.getChange() < 0) {
      scalerListener.workersScaledDown(0 - event.getChange());
    }
  }

  /**
   * deliver the received event to AllJoinedListener
   */
  private void deliverToAllJoinedListener(JobMasterAPI.AllJoined event) {

    allJoinedListener.allWorkersJoined(event.getWorkerInfoList());
  }

  class ResponseMessageHandler implements MessageHandler {

    @Override
    public void onMessage(RequestID id, int workerId, Message message) {

      if (message instanceof JobMasterAPI.RegisterWorkerResponse) {

        LOG.fine("Received a RegisterWorkerResponse message from the master. \n" + message);

        JobMasterAPI.RegisterWorkerResponse responseMessage =
            (JobMasterAPI.RegisterWorkerResponse) message;

        registrationSucceeded = responseMessage.getResult();

        // nothing to do
      } else if (message instanceof JobMasterAPI.JobScaled) {

        LOG.fine("Received " + message.getClass().getSimpleName()
            + " message from the master. \n" + message);

        JobMasterAPI.JobScaled scaledMessage = (JobMasterAPI.JobScaled) message;
        if (scalerListener == null) {
          scaledEventBuffer.add(scaledMessage);
        } else {
          deliverToScalerListener(scaledMessage);
        }

        workerController.scaled(scaledMessage.getChange(), scaledMessage.getNumberOfWorkers());

      } else if (message instanceof JobMasterAPI.AllJoined) {

        JobMasterAPI.AllJoined joinedMessage = (JobMasterAPI.AllJoined) message;
        if (allJoinedListener == null) {
          allJoinedEventBuffer.add(joinedMessage);
        } else {
          deliverToAllJoinedListener(joinedMessage);
        }

      } else {
        LOG.warning("Received message unrecognized. \n" + message);
      }

    }
  }

  public class ClientConnectHandler implements ConnectHandler {
    @Override
    public void onError(SocketChannel channel, StatusCode status) {
      disconnected = true;
    }

    @Override
    public void onConnect(SocketChannel channel) {
      disconnected = false;
      LOG.info("Worker " + thisWorker.getWorkerID() + " connected to JobMaster: " + channel);
    }

    @Override
    public void onClose(SocketChannel channel) {

    }
  }

}
