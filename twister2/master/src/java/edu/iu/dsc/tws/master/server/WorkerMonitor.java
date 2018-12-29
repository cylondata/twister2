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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.Message;

import edu.iu.dsc.tws.common.net.tcp.request.MessageHandler;
import edu.iu.dsc.tws.common.net.tcp.request.RRServer;
import edu.iu.dsc.tws.common.net.tcp.request.RequestID;
import edu.iu.dsc.tws.common.resource.WorkerInfoUtils;
import edu.iu.dsc.tws.master.dashclient.DashboardClient;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI.ListWorkersRequest;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI.ListWorkersResponse;
import edu.iu.dsc.tws.proto.system.job.JobAPI;

/**
 * This class monitors the workers in a job
 * It keeps the worker list and provides to list requests
 */
public class WorkerMonitor implements MessageHandler {
  private static final Logger LOG = Logger.getLogger(WorkerMonitor.class.getName());

  private JobMaster jobMaster;
  private RRServer rrServer;
  private DashboardClient dashClient;

  // this is used to assign next ID to newly registering worker,
  // when job master assigns workerIDs
  private int nextWorkerID = 0;

  private boolean jobMasterAssignsWorkerIDs;
  private int numberOfWorkers;
  private boolean driverRegistered = false;

  private TreeMap<Integer, WorkerWithState> workers;
  private HashMap<Integer, RequestID> waitList;

  public WorkerMonitor(JobMaster jobMaster, RRServer rrServer, DashboardClient dashClient,
                       JobAPI.Job job, boolean jobMasterAssignsWorkerIDs) {
    this.jobMaster = jobMaster;
    this.rrServer = rrServer;
    this.dashClient = dashClient;

    this.numberOfWorkers = job.getNumberOfWorkers();
    this.jobMasterAssignsWorkerIDs = jobMasterAssignsWorkerIDs;

    workers = new TreeMap<>();
    waitList = new HashMap<>();
  }

  /**
   * assign next workerID
   * @return
   */
  private int assignWorkerID() {

    int id = nextWorkerID;
    nextWorkerID++;

    return id;
  }

  /**
   * if this worker already registered with IP and port
   * return the id, otherwise, return -1
   * if a worker is already registered and trying to register again,
   * it means that it is coming from failure
   *
   * we assume that IP:port pair does not change after failure
   * @param workerIP
   * @param port
   * @return
   */
  private int getRegisteredWorkerID(String workerIP, int port) {
    for (WorkerWithState workerWithState: workers.values()) {
      if (workerIP.equals(workerWithState.getIp()) && port == workerWithState.getPort()) {
        return workerWithState.getWorkerID();
      }
    }

    return -1;
  }

  @Override
  public void onMessage(RequestID id, int workerId, Message message) {

    if (message instanceof JobMasterAPI.Ping) {
      JobMasterAPI.Ping ping = (JobMasterAPI.Ping) message;
      pingMessageReceived(id, ping);

    } else if (message instanceof JobMasterAPI.RegisterWorker) {
      JobMasterAPI.RegisterWorker rwMessage = (JobMasterAPI.RegisterWorker) message;
      registerWorkerMessageReceived(id, workerId, rwMessage);

    } else if (message instanceof JobMasterAPI.WorkerStateChange) {
      JobMasterAPI.WorkerStateChange wscMessage = (JobMasterAPI.WorkerStateChange) message;
      stateChangeMessageReceived(id, wscMessage);

    } else if (message instanceof JobMasterAPI.ListWorkersRequest) {
      LOG.log(Level.FINE, "ListWorkersRequest received: " + message.toString());
      JobMasterAPI.ListWorkersRequest listMessage = (JobMasterAPI.ListWorkersRequest) message;
      listWorkersMessageReceived(id, listMessage);

    } else if (message instanceof JobMasterAPI.RegisterDriver) {
      LOG.log(Level.INFO, "RegisterDriver message received: ");
      JobMasterAPI.RegisterDriver registerMessage = (JobMasterAPI.RegisterDriver) message;
      registerDriverMessageReceived(id, registerMessage);

    } else if (message instanceof JobMasterAPI.WorkersScaled) {
      LOG.log(Level.INFO, "WorkersScaled message received: " + message.toString());
      JobMasterAPI.WorkersScaled scaledMessage = (JobMasterAPI.WorkersScaled) message;

      if (scaledMessage.getChange() > 0) {
        scaledUpMessageReceived(id, scaledMessage);
      } else if (scaledMessage.getChange() < 0) {
        scaledDownMessageReceived(id, scaledMessage);
      } else {
        LOG.warning("Scaled message received with change 0. Doing nothing.");
        return;
      }

    } else if (message instanceof JobMasterAPI.Broadcast) {
      LOG.log(Level.INFO, "Broadcast message received from the driver.");
      JobMasterAPI.Broadcast broadcastMessage = (JobMasterAPI.Broadcast) message;
      broadcastMessageReceived(id, broadcastMessage);

    } else if (message instanceof JobMasterAPI.WorkerToDriver) {
      LOG.log(Level.FINE, "WorkerToDriver message received: " + message.toString());
      JobMasterAPI.WorkerToDriver toDriverMessage = (JobMasterAPI.WorkerToDriver) message;
      toDriverMessageReceived(id, toDriverMessage);

    } else {
      LOG.log(Level.SEVERE, "Un-known message received: " + message);
    }
  }

  private void pingMessageReceived(RequestID id, JobMasterAPI.Ping ping) {

    if (workers.containsKey(ping.getWorkerID())) {
      LOG.fine("Ping message received from a worker: \n" + ping);
      workers.get(ping.getWorkerID()).setPingTimestamp(System.currentTimeMillis());
    } else {
      LOG.warning("Ping message received from a worker that has not joined the job yet: " + ping);
    }

    JobMasterAPI.Ping pingResponse = JobMasterAPI.Ping.newBuilder()
        .setWorkerID(ping.getWorkerID())
        .setPingMessage("Ping Response From the Master to Worker")
        .setMessageType(JobMasterAPI.Ping.MessageType.MASTER_TO_WORKER)
        .build();

    rrServer.sendResponse(id, pingResponse);
    LOG.fine("Ping response sent to the worker: \n" + pingResponse);

    // send Ping message to dashboard
    if (dashClient != null) {
      dashClient.workerHeartbeat(ping.getWorkerID());
    }

  }

  private void registerWorkerMessageReceived(RequestID id, int workerId,
                                             JobMasterAPI.RegisterWorker message) {

    LOG.fine("RegisterWorker message received: \n" + message);
    JobMasterAPI.WorkerInfo workerInfo = message.getWorkerInfo();

    // if it is coming from failure
    // update the worker status and return
    int registeredWorkerID = getRegisteredWorkerID(workerInfo.getWorkerIP(), workerInfo.getPort());
    if (registeredWorkerID >= 0) {
      // update the worker status in the worker list
      workers.get(registeredWorkerID).addWorkerState(JobMasterAPI.WorkerState.STARTING);
      LOG.info("WorkerID: " + registeredWorkerID + " joined from failure.");

      // send the response message
      sendRegisterWorkerResponse(id, workerInfo.getWorkerID(), true);

      // send worker registration message to dashboard
      if (dashClient != null) {
        dashClient.registerWorker(workerInfo);
      }

      return;
    }

    // if job master assigns workerIDs, get new id and update it in WorkerInfo
    // also set in RRServer
    if (jobMasterAssignsWorkerIDs) {
      int newWorkerID = assignWorkerID();
      workerInfo = WorkerInfoUtils.updateWorkerID(workerInfo, newWorkerID);
      rrServer.setWorkerChannel(newWorkerID);
    }

    // if it is not coming from failure but workerID already registered
    // something wrong
    if (workers.containsKey(workerInfo.getWorkerID())) {
      LOG.severe("Second RegisterWorker message received for workerID: " + workerInfo.getWorkerID()
          + "\nIgnoring this RegisterWorker message. "
          + "\nReceived Message: " + message
          + "\nPrevious Worker with that workerID: " + workers.get(workerInfo.getWorkerID()));

      // send the response message
      sendRegisterWorkerResponse(id, workerInfo.getWorkerID(), false);

      return;
    }

    // add the worker to worker list
    WorkerWithState worker = new WorkerWithState(workerInfo);
    worker.addWorkerState(JobMasterAPI.WorkerState.STARTING);
    workers.put(worker.getWorkerID(), worker);

    // send success response message
    sendRegisterWorkerResponse(id, worker.getWorkerID(), true);

    // send worker registration message to dashboard
    if (dashClient != null) {
      dashClient.registerWorker(workerInfo);
    }

    // if all workers registered, inform all workers
    if (allWorkersRegistered()) {
      LOG.info("All " + workers.size() + " workers joined the job.");
      sendListWorkersResponseToWaitList();

      sendWorkersJoinedMessage();
    }
  }

  private void registerDriverMessageReceived(RequestID id, JobMasterAPI.RegisterDriver message) {

    if (driverRegistered) {
      JobMasterAPI.RegisterDriverResponse failResponse =
          JobMasterAPI.RegisterDriverResponse.newBuilder()
              .setSucceeded(false)
              .setReason("A driver already registered with JobMaster. Can be at most one driver.")
              .build();
      rrServer.sendResponse(id, failResponse);
      LOG.warning("RegisterDriverResponse sent to the driver: \n" + failResponse);
      return;
    }

    driverRegistered = true;

    JobMasterAPI.RegisterDriverResponse successResponse =
        JobMasterAPI.RegisterDriverResponse.newBuilder()
            .setSucceeded(true)
            .build();
    rrServer.sendResponse(id, successResponse);
    LOG.fine("RegisterDriverResponse sent to the driver: \n" + successResponse);

    // if all workers have already registered,
    // send the driver allWorkersJoined message
    if (allWorkersRegistered()) {
      JobMasterAPI.WorkersJoined joinedMessage = constructWorkersJoinedMessage();
      rrServer.sendMessage(joinedMessage, RRServer.DRIVER_ID);
    }
  }


  private void stateChangeMessageReceived(RequestID id, JobMasterAPI.WorkerStateChange message) {

    // if this worker has not registered
    if (!workers.containsKey(message.getWorkerID())) {

      LOG.warning("WorkerStateChange message received from a worker "
          + "that has not joined the job yet.\n"
          + "Not processing the message, just sending a response"
          + message);

      sendWorkerStateChangeResponse(id, message.getWorkerID(), message.getState());
      return;

    } else if (message.getState() == JobMasterAPI.WorkerState.RUNNING) {
      workers.get(message.getWorkerID()).addWorkerState(message.getState());
      LOG.fine("WorkerStateChange RUNNING message received: \n" + message);

      // send the response message
      sendWorkerStateChangeResponse(id, message.getWorkerID(), message.getState());

      // send worker state change message to dashboard
      if (dashClient != null) {
        dashClient.workerStateChange(message.getWorkerID(), message.getState());
      }

      // if all workers have become RUNNING, send job STARTED message
      if (haveAllWorkersBecomeRunning()) {
        jobMaster.allWorkersBecameRunning();
      }

    } else if (message.getState() == JobMasterAPI.WorkerState.COMPLETED) {

      workers.get(message.getWorkerID()).addWorkerState(message.getState());
      LOG.fine("WorkerStateChange COMPLETED message received: \n" + message);

      // send the response message
      sendWorkerStateChangeResponse(id, message.getWorkerID(), message.getState());

      // send worker state change message to dashboard
      if (dashClient != null) {
        dashClient.workerStateChange(message.getWorkerID(), message.getState());
      }

      // check whether all workers completed
      // if so, stop the job master
      // if all workers have completed, no need to send the response message back to the client
      if (haveAllWorkersCompleted()) {
        jobMaster.completeJob();
      }

      return;

    } else {
      LOG.warning("Unrecognized WorkerStateChange message received. Ignoring and sending reply: \n"
          + message);
      // send the response message
      sendWorkerStateChangeResponse(id, message.getWorkerID(), message.getState());
    }
  }

  private void scaledDownMessageReceived(RequestID id,
                                         JobMasterAPI.WorkersScaled scaledMessage) {

    JobMasterAPI.ScaledResponse scaledResponse = JobMasterAPI.ScaledResponse.newBuilder()
        .setSucceeded(true)
        .build();

    // modify numberOfWorkers
    numberOfWorkers = scaledMessage.getNumberOfWorkers();

    rrServer.sendResponse(id, scaledResponse);
    LOG.fine("ScaledResponse sent to the driver: \n" + scaledResponse);

    // update nextWorkerID
    // since we do not want gaps in workerID sequence,
    // we reuse the deleted IDs
    nextWorkerID = nextWorkerID + scaledMessage.getChange();

    // construct killedWorkers list and remove those workers from workers list
    List<Integer> killedWorkers = new LinkedList<>();

    for (int i = 0; i < (0 - scaledMessage.getChange()); i++) {
      int killedID = numberOfWorkers + i;
      killedWorkers.add(killedID);
      workers.remove(killedID);
      rrServer.removeWorkerChannel(killedID);
    }

    // let all remaining workers know about the scaled message
    for (int workerID : workers.keySet()) {
      rrServer.sendMessage(scaledMessage, workerID);
    }

    // send Scale message to the dashboard
    if (dashClient != null) {
      dashClient.scaledWorkers(
          scaledMessage.getChange(), scaledMessage.getNumberOfWorkers(), killedWorkers);
    }
  }

  private void scaledUpMessageReceived(RequestID id,
                                      JobMasterAPI.WorkersScaled scaledMessage) {

    JobMasterAPI.ScaledResponse scaledResponse = JobMasterAPI.ScaledResponse.newBuilder()
        .setSucceeded(true)
        .build();

    // keep previous numberOfWorkers and update numberOfWorkers with new value
    int numberOfWorkersBeforeScaling = numberOfWorkers;
    numberOfWorkers = scaledMessage.getNumberOfWorkers();

    rrServer.sendResponse(id, scaledResponse);
    LOG.fine("ScaledResponse sent to the driver: \n" + scaledResponse);

    // if all newly scaled up workers are already joined
    // send WorkersJoined messages
    if (allWorkersRegistered()) {
      sendWorkersJoinedMessage();
    }

    // let all previous workers know about the scaled message
    // no need for informing newly added workers
    for (int wID = 0; wID < numberOfWorkersBeforeScaling; wID++) {
      rrServer.sendMessage(scaledMessage, wID);
    }

    // send Scaled message to the dashboard
    if (dashClient != null) {
      dashClient.scaledWorkers(
          scaledMessage.getChange(), scaledMessage.getNumberOfWorkers(), new LinkedList<Integer>());
    }

  }

  private void broadcastMessageReceived(RequestID id, JobMasterAPI.Broadcast broadcastMessage) {

    // if the number of workers in the WorkerMonitor and the incoming message does not match,
    // return a failure message
    if (broadcastMessage.getNumberOfWorkers() != numberOfWorkers) {
      JobMasterAPI.BroadcastResponse failResponse =
          JobMasterAPI.BroadcastResponse.newBuilder()
              .setSucceeded(false)
              .setReason("NumberOfWorkers does not match in the broadcast message and JobMaster")
              .build();
      rrServer.sendResponse(id, failResponse);
      LOG.warning("BroadcastResponse sent to the driver: \n" + failResponse);
      return;
    }

    // if all workers are not registered,
    // send a failure response message to the driver
    // do not send the broadcast message to any workers
    if (!allWorkersRegistered()) {
      JobMasterAPI.BroadcastResponse failResponse =
          JobMasterAPI.BroadcastResponse.newBuilder()
              .setSucceeded(false)
              .setReason("Not all workers are in RUNNING state")
              .build();
      rrServer.sendResponse(id, failResponse);
      LOG.warning("BroadcastResponse sent to the driver: \n" + failResponse);
      return;
    }

    // deliver the broadcast message to all workers
    for (int workerID : workers.keySet()) {
      boolean queued = rrServer.sendMessage(broadcastMessage, workerID);

      // if the message can not be queued, send a failure response
      // this may deliver the broadcast message to some workers but not to all
      // workers may be in an inconsistent state
      // TODO: we may need to find a solution to this
      if (!queued) {
        JobMasterAPI.BroadcastResponse failResponse =
            JobMasterAPI.BroadcastResponse.newBuilder()
                .setSucceeded(false)
                .setReason("Broadcast message can not be sent to workerID: " + workerID)
                .build();
        rrServer.sendResponse(id, failResponse);
        LOG.warning("BroadcastResponse sent to the driver: \n" + failResponse);
        return;
      }
    }

    // TODO: before sending a success response,
    // we need to watch sendComplete events for the broadcast messages to the workers
    // that will make sure that the broadcast message is delivered to all workers
    JobMasterAPI.BroadcastResponse successResponse = JobMasterAPI.BroadcastResponse.newBuilder()
        .setSucceeded(true)
        .build();

    rrServer.sendResponse(id, successResponse);
    LOG.fine("BroadcastResponse sent to the driver: \n" + successResponse);
  }

  private void toDriverMessageReceived(RequestID id, JobMasterAPI.WorkerToDriver toDriverMessage) {

    // first send the received message to the driver
    boolean queued = rrServer.sendMessage(toDriverMessage, RRServer.DRIVER_ID);

    // if the message can not be queued to send to the driver,
    // it is because the driver is not connected to the worker
    if (!queued) {
      JobMasterAPI.WorkerToDriverResponse failResponse =
          JobMasterAPI.WorkerToDriverResponse.newBuilder()
              .setSucceeded(false)
              .setReason("Driver is not connected to JobMaster")
              .build();
      rrServer.sendResponse(id, failResponse);
      LOG.warning("WorkerToDriverResponse sent to the driver: \n" + failResponse);
      return;
    }

    JobMasterAPI.WorkerToDriverResponse successResponse =
        JobMasterAPI.WorkerToDriverResponse.newBuilder()
            .setSucceeded(true)
            .build();

    rrServer.sendResponse(id, successResponse);
    LOG.fine("WorkerToDriverResponse sent to the driver: \n" + successResponse);
  }

  /**
   * make sure that
   * all workers registered and their state may be anything
   */
  private boolean allWorkersRegistered() {

    // if numberOfWorkers does not match the number of registered workers,
    // return false
    if (workers.size() != numberOfWorkers) {
      return false;
    }

    // if there is a gap in workerID sequence, return false
    // since workerIDs are sorted and they start from 0
    // checking the workerID of the last worker is sufficient
    if (workers.lastKey() != (numberOfWorkers - 1)) {
      return false;
    }

    return true;
  }

  /**
   * make sure that
   * all workers registered and their state is RUNNING
   * so that we can send a message to all
   */
  private boolean allWorkersRunning() {

    if (!allWorkersRegistered()) {
      return false;
    }

    // check the status of all workers, all have to be RUNNING
    for (WorkerWithState worker : workers.values()) {
      if (worker.getLastState() != JobMasterAPI.WorkerState.RUNNING) {
        return false;
      }
    }

    return true;
  }

  /**
   * worker RUNNING message received from all workers
   * if some workers may have already completed, that does not matter
   * the important thing is whether they have became RUNNING in the past
   */
  private boolean haveAllWorkersBecomeRunning() {
    if (numberOfWorkers != workers.size()) {
      return false;
    }

    for (WorkerWithState worker : workers.values()) {
      if (!worker.hasWorkerBecomeRunning()) {
        return false;
      }
    }

    return true;
  }

  /**
   * worker COMPLETED message received from all workers
   */
  private boolean haveAllWorkersCompleted() {
    if (numberOfWorkers != workers.size()) {
      return false;
    }

    for (WorkerWithState worker : workers.values()) {
      if (!worker.hasWorkerCompleted()) {
        return false;
      }
    }

    return true;
  }

  private void sendRegisterWorkerResponse(RequestID id, int workerID, boolean result) {

    JobMasterAPI.RegisterWorkerResponse response =
        JobMasterAPI.RegisterWorkerResponse.newBuilder()
            .setWorkerID(workerID)
            .setResult(result)
            .build();

    rrServer.sendResponse(id, response);
    LOG.fine("RegisterWorkerResponse sent:\n" + response);
  }

  private void sendWorkerStateChangeResponse(RequestID id, int workerID,
                                             JobMasterAPI.WorkerState sentState) {

    JobMasterAPI.WorkerStateChangeResponse response =
        JobMasterAPI.WorkerStateChangeResponse.newBuilder()
            .setWorkerID(workerID)
            .setState(sentState)
            .build();

    rrServer.sendResponse(id, response);
    LOG.fine("WorkerStateChangeResponse sent:\n" + response);

  }

  private void listWorkersMessageReceived(RequestID id, ListWorkersRequest listMessage) {

    if (listMessage.getRequestType() == ListWorkersRequest.RequestType.IMMEDIATE_RESPONSE) {

      sendListWorkersResponse(listMessage.getWorkerID(), id);
      LOG.log(Level.FINE, String.format("Expecting %d workers, %d joined",
          numberOfWorkers, workers.size()));
    } else if (listMessage.getRequestType()
        == JobMasterAPI.ListWorkersRequest.RequestType.RESPONSE_AFTER_ALL_JOINED) {

      // if all workers have already joined, send the current list
      if (workers.size() == numberOfWorkers) {
        sendListWorkersResponse(listMessage.getWorkerID(), id);

        // if some workers have not joined yet, put this worker into the wait list
      } else {
        waitList.put(listMessage.getWorkerID(), id);
      }

      LOG.log(Level.FINE, String.format("Expecting %d workers, %d joined",
          numberOfWorkers, workers.size()));
    }
  }

  private void sendListWorkersResponse(int workerID, RequestID requestID) {

    JobMasterAPI.ListWorkersResponse.Builder responseBuilder = ListWorkersResponse.newBuilder()
        .setWorkerID(workerID);

    for (WorkerWithState worker : workers.values()) {
      responseBuilder.addWorker(worker.getWorkerInfo());
    }

    JobMasterAPI.ListWorkersResponse response = responseBuilder.build();
    rrServer.sendResponse(requestID, response);
    LOG.fine("ListWorkersResponse sent:\n" + response);
  }

  private void sendListWorkersResponseToWaitList() {
    for (Map.Entry<Integer, RequestID> entry : waitList.entrySet()) {
      sendListWorkersResponse(entry.getKey(), entry.getValue());
    }

    waitList.clear();
  }

  /**
   * send WorkersJoined message to all workers and the driver
   */
  private void sendWorkersJoinedMessage() {

    LOG.info("Sending WorkersJoined messages ...");

    JobMasterAPI.WorkersJoined joinedMessage = constructWorkersJoinedMessage();

    // send the message to the driver, if any
    // if there is no driver, no problem, this method will return false
    rrServer.sendMessage(joinedMessage, RRServer.DRIVER_ID);

    // send the message to all workers
    for (Integer workerID : workers.keySet()) {
      rrServer.sendMessage(joinedMessage, workerID);
    }

  }

  /**
   * construct WorkersJoined message
   */
  private JobMasterAPI.WorkersJoined constructWorkersJoinedMessage() {

    JobMasterAPI.WorkersJoined.Builder joinedBuilder = JobMasterAPI.WorkersJoined.newBuilder()
        .setNumberOfWorkers(numberOfWorkers);

    for (WorkerWithState worker : workers.values()) {
      joinedBuilder.addWorker(worker.getWorkerInfo());
    }

    return joinedBuilder.build();
  }

}
