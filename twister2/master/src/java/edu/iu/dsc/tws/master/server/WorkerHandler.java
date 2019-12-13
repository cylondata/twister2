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

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.Message;

import edu.iu.dsc.tws.api.net.request.MessageHandler;
import edu.iu.dsc.tws.api.net.request.RequestID;
import edu.iu.dsc.tws.common.net.tcp.request.RRServer;
import edu.iu.dsc.tws.common.zk.WorkerWithState;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;

/**
 * Handles JobMaster to worker messaging
 * It handles following message types:
 * JobMasterAPI.RegisterWorker
 * JobMasterAPI.WorkerStateChange
 * JobMasterAPI.ListWorkersRequest
 * <p>
 * It gets request messages from workers and talks to WorkerMonitor
 * It sends response messages back to workers
 */

public class WorkerHandler implements MessageHandler {

  private static final Logger LOG = Logger.getLogger(WorkerHandler.class.getName());

  private WorkerMonitor workerMonitor;
  private RRServer rrServer;
  private boolean zkUsed;

  // shows whether all workers connected to the job master
  // it is initially false, it becomes true after all workers connected
  // it also becomes false after scale-up and it becomes true when new workers connected
  private boolean allConnected = false;

  private HashMap<Integer, RequestID> waitList;

  public WorkerHandler(WorkerMonitor workerMonitor, RRServer rrServer, boolean zkUsed) {
    this.workerMonitor = workerMonitor;
    this.rrServer = rrServer;
    this.zkUsed = zkUsed;

    waitList = new HashMap<>();
  }

  public boolean isAllConnected() {
    return allConnected;
  }

  @Override
  public void onMessage(RequestID id, int workerId, Message message) {

    if (message instanceof JobMasterAPI.RegisterWorker) {

      JobMasterAPI.RegisterWorker rwMessage = (JobMasterAPI.RegisterWorker) message;
      registerWorkerMessageReceived(id, rwMessage);

    } else if (message instanceof JobMasterAPI.WorkerStateChange) {

      JobMasterAPI.WorkerStateChange scMessage = (JobMasterAPI.WorkerStateChange) message;
      stateChangeMessageReceived(id, scMessage);

    } else if (message instanceof JobMasterAPI.ListWorkersRequest) {
      LOG.log(Level.FINE, "ListWorkersRequest received: " + message.toString());
      JobMasterAPI.ListWorkersRequest listMessage = (JobMasterAPI.ListWorkersRequest) message;
      listWorkersMessageReceived(id, listMessage);

    } else {
      LOG.log(Level.SEVERE, "Un-known message type received: " + message);
    }

  }

  private void registerWorkerMessageReceived(RequestID id, JobMasterAPI.RegisterWorker message) {

    // if all workers connected, set it
    handleAllConnected();

    if (zkUsed) {
      int wID = message.getWorkerInfo().getWorkerID();
      LOG.fine("Since ZooKeeper is used, ignoring RegisterWorker message for worker: " + wID);
      sendRegisterWorkerResponse(id, wID, true, null);

      // if all workers connected with this worker,
      // publish all joined event to the driver if exists
      // if zk is not used, this is handled in workerMonitor
      workerMonitor.informDriverForAllJoined();

      return;
    }

    LOG.fine("RegisterWorker message received: \n" + message);
    JobMasterAPI.WorkerInfo workerInfo = message.getWorkerInfo();
    boolean initialAllJoined = workerMonitor.isAllJoined();
    WorkerWithState workerWithState = new WorkerWithState(workerInfo, message.getInitialState());

    if (message.getInitialState() == JobMasterAPI.WorkerState.RESTARTED) {
      // if it is coming from failure
      String failMessage = workerMonitor.restarted(workerWithState);
      if (failMessage != null) {
        sendRegisterWorkerResponse(id, workerInfo.getWorkerID(), false, failMessage);
        return;
      }

    } else {

      // if it is not coming from failure
      String failMessage = workerMonitor.started(workerWithState);
      if (failMessage != null) {
        sendRegisterWorkerResponse(id, workerInfo.getWorkerID(), false, failMessage);
        return;
      }
    }

    // send a success response
    sendRegisterWorkerResponse(id, workerInfo.getWorkerID(), true, null);

    // if all workers registered, inform all workers
    if (!initialAllJoined && workerMonitor.isAllJoined()) {
      LOG.info("All workers joined the job. Worker IDs: " + workerMonitor.getWorkerIDs());
      sendListWorkersResponseToWaitList();

      sendWorkersJoinedMessage();
    }

  }

  private void stateChangeMessageReceived(RequestID id, JobMasterAPI.WorkerStateChange message) {

    // if this worker has not registered
    if (!workerMonitor.existWorker(message.getWorkerID())) {

      LOG.warning("WorkerStateChange message received from a worker "
          + "that has not joined the job yet.\n"
          + "Not processing the message, just sending a response"
          + message);

      sendWorkerStateChangeResponse(id, message.getWorkerID(), message.getState());
      return;

    } else if (message.getState() == JobMasterAPI.WorkerState.COMPLETED) {

      // send the response message first
      // then inform WorkerMonitor
      // if this is the last worker to complete
      // WorkerMonitor will delete the job also
      sendWorkerStateChangeResponse(id, message.getWorkerID(), message.getState());

      workerMonitor.completed(message.getWorkerID());
      return;

    } else if (message.getState() == JobMasterAPI.WorkerState.FAILED) {

      // TODO: we do not expect this message to arrive
      // Failed workers can not send this message
      // But we implement it anyway
      LOG.warning("Worker [" + message.getWorkerID() + "] Failed. ");

      sendWorkerStateChangeResponse(id, message.getWorkerID(), message.getState());

      workerMonitor.failed(message.getWorkerID());
      return;

    } else {
      LOG.warning("Unrecognized WorkerStateChange message received. Ignoring and sending reply: \n"
          + message);
      // send the response message
      sendWorkerStateChangeResponse(id, message.getWorkerID(), message.getState());
    }
  }

  private void listWorkersMessageReceived(RequestID id,
                                          JobMasterAPI.ListWorkersRequest listMessage) {

    if (listMessage.getRequestType()
        == JobMasterAPI.ListWorkersRequest.RequestType.IMMEDIATE_RESPONSE) {

      sendListWorkersResponse(listMessage.getWorkerID(), id);
      LOG.fine(String.format("Expecting %d workers, %d joined",
          workerMonitor.getNumberOfWorkers(), workerMonitor.getWorkersListSize()));

    } else if (listMessage.getRequestType()
        == JobMasterAPI.ListWorkersRequest.RequestType.RESPONSE_AFTER_ALL_JOINED) {

      // if all workers have already joined, send the current list
      if (workerMonitor.getWorkersListSize() == workerMonitor.getNumberOfWorkers()) {
        sendListWorkersResponse(listMessage.getWorkerID(), id);

        // if some workers have not joined yet, put this worker into the wait list
      } else {
        waitList.put(listMessage.getWorkerID(), id);
      }

      LOG.log(Level.FINE, String.format("Expecting %d workers, %d joined",
          workerMonitor.getNumberOfWorkers(), workerMonitor.getWorkersListSize()));
    }
  }

  /**
   * workerMonitor calls this method when job is scaled down and zk is not used
   * @param instancesRemoved
   */
  public void workersScaledDown(int instancesRemoved) {

    int change = 0 - instancesRemoved;
    // construct scaled message to send to workers
    JobMasterAPI.WorkersScaled scaledMessage = JobMasterAPI.WorkersScaled.newBuilder()
        .setChange(change)
        .setNumberOfWorkers(workerMonitor.getNumberOfWorkers())
        .build();

    // let all remaining workers know about the scaled message
    for (int workerID : workerMonitor.getWorkerIDs()) {
      rrServer.sendMessage(scaledMessage, workerID);
    }
  }

  /**
   * workerMonitor calls this method when the job is scaled up
   * @param instancesAdded
   */
  public void workersScaledUp(int instancesAdded) {

    unsetAllConnected();

    if (zkUsed) {
      return;
    }

    JobMasterAPI.WorkersScaled scaledMessage = JobMasterAPI.WorkersScaled.newBuilder()
        .setChange(instancesAdded)
        .setNumberOfWorkers(workerMonitor.getNumberOfWorkers())
        .build();

    int numberOfWorkersBeforeScaling = workerMonitor.getNumberOfWorkers() - instancesAdded;
    // let all previous workers know about the scaled message
    // no need for informing newly added workers
    for (int wID = 0; wID < numberOfWorkersBeforeScaling; wID++) {
      rrServer.sendMessage(scaledMessage, wID);
    }

  }

  private void sendListWorkersResponse(int workerID, RequestID requestID) {

    JobMasterAPI.ListWorkersResponse.Builder responseBuilder =
        JobMasterAPI.ListWorkersResponse.newBuilder()
            .setWorkerID(workerID);

    for (WorkerWithState worker : workerMonitor.getWorkerList()) {
      responseBuilder.addWorker(worker.getInfo());
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


  private void sendRegisterWorkerResponse(RequestID id,
                                          int workerID,
                                          boolean result,
                                          String reason) {

    JobMasterAPI.RegisterWorkerResponse response =
        JobMasterAPI.RegisterWorkerResponse.newBuilder()
            .setWorkerID(workerID)
            .setResult(result)
            .setReason(reason == null ? "" : reason)
            .build();

    rrServer.sendResponse(id, response);
    LOG.fine("RegisterWorkerResponse sent:\n" + response);
  }

  private void sendWorkerStateChangeResponse(RequestID id,
                                             int workerID,
                                             JobMasterAPI.WorkerState sentState) {

    JobMasterAPI.WorkerStateChangeResponse response =
        JobMasterAPI.WorkerStateChangeResponse.newBuilder()
            .setWorkerID(workerID)
            .setState(sentState)
            .build();

    rrServer.sendResponse(id, response);
    LOG.fine("WorkerStateChangeResponse sent:\n" + response);

  }

  /**
   * send WorkersJoined message to all workers and the driver
   */
  public void sendWorkersJoinedMessage() {

    LOG.info("Sending WorkersJoined messages ...");

    List<JobMasterAPI.WorkerInfo> workerInfoList = workerMonitor.getWorkerInfoList();

    JobMasterAPI.WorkersJoined joinedMessage = JobMasterAPI.WorkersJoined.newBuilder()
        .setNumberOfWorkers(workerInfoList.size())
        .addAllWorker(workerInfoList)
        .build();

    // send the message to all workers
    for (JobMasterAPI.WorkerInfo workerInfo : workerInfoList) {
      rrServer.sendMessage(joinedMessage, workerInfo.getWorkerID());
    }
  }

  /**
   * return true all all workers connected
   * @return
   */
  private boolean allWorkersConnected() {
    int numberOfWorkers = workerMonitor.getNumberOfWorkers();
    Set<Integer> connectedWorkers = rrServer.getConnectedWorkers();
    if (connectedWorkers.size() == numberOfWorkers
        && Collections.max(connectedWorkers) == numberOfWorkers - 1) {
      return true;
    }
    return false;
  }

  /**
   * if all connected, set allConnected flag
   */
  private void handleAllConnected() {
    if (!allConnected && allWorkersConnected()) {
      allConnected = true;
    }
  }

  /**
   * this is called after worker scale-up
   * sets allConnected to false
   */
  public void unsetAllConnected() {
    // first check whether all connected
    // this is possible but very unlikely
    if (allWorkersConnected()) {
      return;
    }

    allConnected = false;
  }

}
