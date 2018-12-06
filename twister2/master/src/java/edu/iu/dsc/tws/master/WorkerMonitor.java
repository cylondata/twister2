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

import java.util.HashMap;
import java.util.Map;
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

public class WorkerMonitor implements MessageHandler {
  private static final Logger LOG = Logger.getLogger(WorkerMonitor.class.getName());

  private JobMaster jobMaster;
  private RRServer rrServer;
  private DashboardClient dashClient;

  private boolean jobMasterAssignsWorkerIDs;
  private int numberOfWorkers;

  private HashMap<Integer, WorkerWithState> workers;
  private HashMap<Integer, RequestID> waitList;

  public WorkerMonitor(JobMaster jobMaster, RRServer rrServer, DashboardClient dashClient,
                       int numWorkers, boolean jobMasterAssignsWorkerIDs) {
    this.jobMaster = jobMaster;
    this.rrServer = rrServer;
    this.dashClient = dashClient;

    this.numberOfWorkers = numWorkers;
    this.jobMasterAssignsWorkerIDs = jobMasterAssignsWorkerIDs;

    workers = new HashMap<>();
    waitList = new HashMap<>();
  }

  @Override
  public void onMessage(RequestID id, int workerId, Message message) {

    if (message instanceof JobMasterAPI.Ping) {
      JobMasterAPI.Ping ping = (JobMasterAPI.Ping) message;
      pingMessageReceived(id, ping);

    } else if (message instanceof JobMasterAPI.WorkerStateChange) {
      JobMasterAPI.WorkerStateChange wscMessage = (JobMasterAPI.WorkerStateChange) message;
      stateChangeMessageReceived(id, wscMessage);

    } else if (message instanceof JobMasterAPI.ListWorkersRequest) {
      LOG.log(Level.INFO, "ListWorkersRequest received: " + message.toString());
      JobMasterAPI.ListWorkersRequest listMessage = (JobMasterAPI.ListWorkersRequest) message;
      listWorkersMessageReceived(id, listMessage);

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
  }

  private void stateChangeMessageReceived(RequestID id, JobMasterAPI.WorkerStateChange message) {

    if (message.getNewState() == JobMasterAPI.WorkerState.STARTING) {
      LOG.info("WorkerStateChange STARTING message received: \n" + message);
      JobMasterAPI.WorkerInfo workerInfo = message.getWorkerInfo();

      if (jobMasterAssignsWorkerIDs) {
        int workerID = workers.size();
        workerInfo = WorkerInfoUtils.updateWorkerID(workerInfo, workerID);
      }

      WorkerWithState worker = new WorkerWithState(workerInfo);
      worker.addWorkerState(JobMasterAPI.WorkerState.STARTING);
      if (workers.containsKey(worker.getWorkerID())) {
        LOG.severe("Second worker STARTING message received for workerID: " + worker.getWorkerID()
            + "\nIgnoring this worker STARTING message. "
            + "\nReceived Message: " + message
            + "\nPrevious Worker with that workerID: " + workers.get(worker.getWorkerID()));
      } else {
        workers.put(worker.getWorkerID(), worker);
      }
      sendWorkerStateChangeResponse(id, worker.getWorkerID(), message.getNewState());

      // if all workers registered, inform all workers
      if (workers.size() == numberOfWorkers) {
        sendListWorkersResponseToWaitList();
      }

      return;

    } else if (!workers.containsKey(message.getWorkerInfo().getWorkerID())) {

      LOG.warning("WorkerStateChange message received from a worker "
          + "that has not joined the job yet.\n"
          + "Not processing the message, just sending a response"
          + message);

      sendWorkerStateChangeResponse(id, message.getWorkerInfo().getWorkerID(),
          message.getNewState());
      return;

    } else if (message.getNewState() == JobMasterAPI.WorkerState.RUNNING) {
      workers.get(message.getWorkerID()).addWorkerState(message.getNewState());
      LOG.info("WorkerStateChange RUNNING message received: \n" + message);

      // send the response message
      sendWorkerStateChangeResponse(id, message.getWorkerID(), message.getNewState());

      // if all workers have become RUNNING, send job STARTED message
      if (haveAllWorkersBecomeRunning()) {
        jobMaster.allWorkersBecameRunning();
      }

    } else if (message.getNewState() == JobMasterAPI.WorkerState.COMPLETED) {

      workers.get(message.getWorkerID()).addWorkerState(message.getNewState());
      LOG.info("WorkerStateChange COMPLETED message received: \n" + message);

      // send the response message
      sendWorkerStateChangeResponse(id, message.getWorkerID(), message.getNewState());

      // check whether all workers completed
      // if so, stop the job master
      // if all workers have completed, no need to send the response message back to the client
      if (haveAllWorkersCompleted()) {
        jobMaster.allWorkersCompleted();
      }

      return;

    } else {
      LOG.warning("Unrecognized WorkerStateChange message received. Ignoring and sending reply: \n"
          + message);
      // send the response message
      sendWorkerStateChangeResponse(id, message.getWorkerID(), message.getNewState());
    }
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

    for (WorkerWithState worker: workers.values()) {
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

    for (WorkerWithState worker: workers.values()) {
      if (!worker.hasWorkerCompleted()) {
        return false;
      }
    }

    return true;
  }

  private void sendWorkerStateChangeResponse(RequestID id, int workerID,
                                             JobMasterAPI.WorkerState sentState) {

    JobMasterAPI.WorkerStateChangeResponse response =
        JobMasterAPI.WorkerStateChangeResponse.newBuilder()
        .setWorkerID(workerID)
        .setSentState(sentState)
        .build();

    rrServer.sendResponse(id, response);
    LOG.info("WorkerStateChangeResponse sent:\n" + response);

  }

  private void listWorkersMessageReceived(RequestID id, ListWorkersRequest listMessage) {

    if (listMessage.getRequestType() == ListWorkersRequest.RequestType.IMMEDIATE_RESPONSE) {

      sendListWorkersResponse(listMessage.getWorkerID(), id);
      LOG.log(Level.INFO, String.format("Expecting %d workers, %d joined",
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

      LOG.log(Level.INFO, String.format("Expecting %d workers, %d joined",
          numberOfWorkers, workers.size()));
    }
  }

  private void sendListWorkersResponse(int workerID, RequestID requestID) {

    JobMasterAPI.ListWorkersResponse.Builder responseBuilder = ListWorkersResponse.newBuilder()
        .setWorkerID(workerID);

    for (WorkerWithState worker: workers.values()) {
      responseBuilder.addWorker(worker.getWorkerInfo());
    }

    JobMasterAPI.ListWorkersResponse response = responseBuilder.build();
    rrServer.sendResponse(requestID, response);
    LOG.info("ListWorkersResponse sent:\n" + response);
  }

  private void sendListWorkersResponseToWaitList() {
    for (Map.Entry<Integer, RequestID> entry: waitList.entrySet()) {
      sendListWorkersResponse(entry.getKey(), entry.getValue());
    }

    waitList.clear();
  }

}
