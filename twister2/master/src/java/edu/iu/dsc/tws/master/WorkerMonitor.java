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

import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.Message;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.net.tcp.request.MessageHandler;
import edu.iu.dsc.tws.common.net.tcp.request.RRServer;
import edu.iu.dsc.tws.common.net.tcp.request.RequestID;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI.ListWorkersRequest;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI.ListWorkersResponse;

public class WorkerMonitor implements MessageHandler {
  private static final Logger LOG = Logger.getLogger(WorkerMonitor.class.getName());

  private JobMaster jobMaster;
  private RRServer rrServer;
  private Config config;

  private int numberOfWorkers;

  private HashMap<Integer, WorkerInfo> workers;
  private HashMap<Integer, RequestID> waitList;

  public WorkerMonitor(Config config, JobMaster jobMaster, RRServer rrServer) {
    this(config, jobMaster, rrServer, JobMasterContext.workerInstances(config));
  }

  public WorkerMonitor(Config config, JobMaster jobMaster,
                       RRServer rrServer, int numWorkers) {
    this.config = config;
    this.jobMaster = jobMaster;
    this.rrServer = rrServer;
    this.numberOfWorkers = numWorkers;

    workers = new HashMap<>();
    waitList = new HashMap<>();
  }

  @Override
  public void onMessage(RequestID id, int workerId, Message message) {

    if (message instanceof JobMasterAPI.Ping) {
      JobMasterAPI.Ping ping = (JobMasterAPI.Ping) message;
      pingMessageReceived(id, ping);

    } else if (message instanceof JobMasterAPI.WorkerStateChange) {
      LOG.log(Level.INFO, "Worker change request received: " + message.toString());
      JobMasterAPI.WorkerStateChange wscMessage = (JobMasterAPI.WorkerStateChange) message;
      stateChangeMessageReceived(id, wscMessage);

    } else if (message instanceof JobMasterAPI.ListWorkersRequest) {
      LOG.log(Level.INFO, "List worker request received: " + message.toString());
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
      LOG.info("WorkerStateChange message received: \n" + message);
      InetAddress ip = WorkerInfo.covertToIPAddress(message.getWorkerNetworkInfo().getWorkerIP());
      int port = message.getWorkerNetworkInfo().getPort();
      int workerID = message.getWorkerNetworkInfo().getWorkerID();
      String nodeIP = message.getWorkerNetworkInfo().getNodeIP();
      String rackName = message.getWorkerNetworkInfo().getRackName();
      String dcName = message.getWorkerNetworkInfo().getDataCenterName();

      if (JobMasterContext.jobMasterAssignsWorkerIDs(config)) {
        workerID = workers.size();
      }

      WorkerInfo worker = new WorkerInfo(workerID, ip, port, nodeIP, rackName, dcName);
      worker.setWorkerState(JobMasterAPI.WorkerState.STARTING);
      workers.put(workerID, worker);

      sendWorkerStateChangeResponse(id, workerID, message.getNewState());

      if (workers.size() == numberOfWorkers) {
        sendListWorkersResponseToWaitList();
      }

      return;

    } else if (!workers.containsKey(message.getWorkerNetworkInfo().getWorkerID())) {

      LOG.warning("WorkerStateChange message received from a worker "
          + "that has not joined the job yet.\n"
          + "Not processing the message, just sending a response"
          + message);

      sendWorkerStateChangeResponse(id, message.getWorkerNetworkInfo().getWorkerID(),
          message.getNewState());
      return;

    } else if (message.getNewState() == JobMasterAPI.WorkerState.COMPLETED) {

      workers.get(message.getWorkerNetworkInfo().getWorkerID())
          .setWorkerState(message.getNewState());
      LOG.info("WorkerStateChange message received: \n" + message);

      // send the response message
      sendWorkerStateChangeResponse(id, message.getWorkerNetworkInfo().getWorkerID(),
          message.getNewState());

      // check whether all workers completed
      // if so, stop the job master
      // if all workers have completed, no need to send the response message back to the client
      if (haveAllWorkersCompleted()) {
        jobMaster.allWorkersCompleted();
      }

      return;

    } else {
      workers.get(message.getWorkerNetworkInfo().getWorkerID())
          .setWorkerState(message.getNewState());
      LOG.info("WorkerStateChange message received: \n" + message);

      // send the response message
      sendWorkerStateChangeResponse(id, message.getWorkerNetworkInfo().getWorkerID(),
          message.getNewState());
    }
  }

  private boolean haveAllWorkersCompleted() {
    if (numberOfWorkers != workers.size()) {
      return false;
    }

    for (WorkerInfo worker: workers.values()) {
      if (worker.getWorkerState() != JobMasterAPI.WorkerState.COMPLETED) {
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

    for (WorkerInfo worker: workers.values()) {
      JobMasterAPI.WorkerNetworkInfo.Builder workerInfoBuilder =
          JobMasterAPI.WorkerNetworkInfo.newBuilder()
              .setWorkerID(worker.getWorkerID())
              .setWorkerIP(worker.getIp().getHostAddress())
              .setPort(worker.getPort());

      if (worker.hasNodeIP()) {
        workerInfoBuilder.setNodeIP(worker.getNodeIP());
      }

      if (worker.hasRackName()) {
        workerInfoBuilder.setRackName(worker.getRackName());
      }

      if (worker.hasDataCenterName()) {
        workerInfoBuilder.setDataCenterName(worker.getDataCenterName());
      }

      responseBuilder.addWorkers(workerInfoBuilder.build());
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
