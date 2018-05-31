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
import java.util.logging.Logger;

import com.google.protobuf.Message;

import edu.iu.dsc.tws.common.net.tcp.request.MessageHandler;
import edu.iu.dsc.tws.common.net.tcp.request.RRServer;
import edu.iu.dsc.tws.common.net.tcp.request.RequestID;
import edu.iu.dsc.tws.proto.network.Network;

public class WorkerMonitor implements MessageHandler {
  private static final Logger LOG = Logger.getLogger(JobMaster.class.getName());

  private HashMap<Integer, WorkerInfo> workers;
  private RRServer rrServer;
  private HashMap<Integer, RequestID> waitList;
  private int numberOfWorkers;

  public WorkerMonitor(RRServer rrServer, int numberOfWorkers) {
    this.rrServer = rrServer;
    this.numberOfWorkers = numberOfWorkers;

    workers = new HashMap<>();
    waitList = new HashMap<>();
  }

  @Override
  public void onMessage(RequestID id, int workerId, Message message) {

    if (message instanceof Network.Ping) {

      Network.Ping ping = (Network.Ping) message;
      pingMessageReceived(id, ping);

    } else if (message instanceof Network.WorkerStateChange) {

      Network.WorkerStateChange wscMessage = (Network.WorkerStateChange) message;
      stateChangeMessageReceived(id, wscMessage);

    } else if (message instanceof Network.ListWorkersRequest) {

      Network.ListWorkersRequest listMessage = (Network.ListWorkersRequest) message;
      listWorkersMessageReceived(id, listMessage);

    }
  }

  private void pingMessageReceived(RequestID id, Network.Ping ping) {

    if (workers.containsKey(ping.getWorkerID())) {
      LOG.info("Ping message received from a worker: \n" + ping);
      workers.get(ping.getWorkerID()).setPingTimestamp(System.currentTimeMillis());
    } else {
      LOG.info("Ping message received from a worker that has not joined the job yet: " + ping);
    }

    Network.Ping pingResponse = Network.Ping.newBuilder()
        .setWorkerID(ping.getWorkerID())
        .setPingMessage("Ping Response From the Master to Worker")
        .setMessageType(Network.Ping.MessageType.MASTER_TO_WORKER)
        .build();

    rrServer.sendResponse(id, pingResponse);
    LOG.info("Ping response sent to the worker: \n" + pingResponse);
  }

  private void stateChangeMessageReceived(RequestID id, Network.WorkerStateChange message) {

    if (message.getNewState() == Network.WorkerState.STARTING) {
      InetAddress ip = WorkerInfo.covertToIPAddress(message.getIp());
      int port = message.getPort();
      WorkerInfo worker = new WorkerInfo(message.getWorkerID(), ip, port);
      worker.setWorkerState(Network.WorkerState.STARTING);
      workers.put(message.getWorkerID(), worker);
      LOG.info("WorkerStateChange message received: \n" + message);

      if (workers.size() == numberOfWorkers) {
        sendListWorkersResponseToWaitList();
      }

    } else {
      if (workers.containsKey(message.getWorkerID())) {
        workers.get(message.getWorkerID()).setWorkerState(message.getNewState());
        LOG.info("WorkerStateChange message received: \n" + message);

      } else {
        LOG.warning("WorkerStateChange message received from a worker "
            + "that has not joined the job yet.\n" + message);
      }
    }

    Network.WorkerStateChangeResponse response = Network.WorkerStateChangeResponse.newBuilder()
        .setWorkerID(message.getWorkerID())
        .build();

    rrServer.sendResponse(id, response);
    LOG.info("WorkerStateChangeResponse sent:\n" + response);
  }

  private void listWorkersMessageReceived(RequestID id, Network.ListWorkersRequest listMessage) {

    if (listMessage.getRequestType() == Network.ListWorkersRequest.RequestType.IMMEDIATE_RESPONSE) {

      sendListWorkersResponse(listMessage.getWorkerID(), id);
    } else if (listMessage.getRequestType()
        == Network.ListWorkersRequest.RequestType.RESPONSE_AFTER_ALL_JOINED) {

      // if all workers already joined, send the current list
      if (workers.size() == numberOfWorkers) {

        sendListWorkersResponse(listMessage.getWorkerID(), id);

        // if some workers have not yet joined, put this worker into the wait list
      } else {

        waitList.put(listMessage.getWorkerID(), id);
      }
    }
  }

  private void sendListWorkersResponse(int workerID, RequestID requestID) {

    Network.ListWorkersResponse.Builder responseBuilder = Network.ListWorkersResponse.newBuilder()
        .setWorkerID(workerID);

    for (WorkerInfo worker: workers.values()) {
      Network.ListWorkersResponse.WorkerNetworkInfo workerInfo =
          Network.ListWorkersResponse.WorkerNetworkInfo.newBuilder()
              .setId(worker.getWorkerID())
              .setIp(worker.getIp().getHostAddress())
              .setPort(worker.getPort())
              .build();

      responseBuilder.addWorkers(workerInfo);
    }

    Network.ListWorkersResponse response = responseBuilder.build();
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
