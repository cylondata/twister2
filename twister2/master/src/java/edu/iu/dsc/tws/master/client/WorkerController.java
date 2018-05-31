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

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.Message;

import edu.iu.dsc.tws.common.net.tcp.request.MessageHandler;
import edu.iu.dsc.tws.common.net.tcp.request.RRClient;
import edu.iu.dsc.tws.common.net.tcp.request.RequestID;
import edu.iu.dsc.tws.proto.network.Network.ListWorkersRequest;
import edu.iu.dsc.tws.proto.network.Network.ListWorkersResponse;
import edu.iu.dsc.tws.rsched.bootstrap.IWorkerController;
import edu.iu.dsc.tws.rsched.bootstrap.WorkerNetworkInfo;

public class WorkerController implements IWorkerController, MessageHandler {
  private static final Logger LOG = Logger.getLogger(WorkerController.class.getName());

  private WorkerNetworkInfo thisWorker;
  private ArrayList<WorkerNetworkInfo> workerList;
  private int numberOfWorkers;

  private RRClient rrClient;
  private BooleanObject responseReceived = new BooleanObject(false);

  public WorkerController(WorkerNetworkInfo thisWorker, int numberOfWorkers, RRClient rrClient) {
    this.numberOfWorkers = numberOfWorkers;
    this.thisWorker = thisWorker;
    this.rrClient = rrClient;
    workerList = new ArrayList<>();
    workerList.add(thisWorker);
  }

  @Override
  public WorkerNetworkInfo getWorkerNetworkInfo() {
    return thisWorker;
  }

  @Override
  public WorkerNetworkInfo getWorkerNetworkInfoForID(int id) {
    for (WorkerNetworkInfo info: workerList) {
      if (info.getWorkerID() == id) {
        return info;
      }
    }

    return null;
  }

  @Override
  public int getNumberOfWorkers() {
    return numberOfWorkers;
  }

  @Override
  public List<WorkerNetworkInfo> getWorkerList() {

    if (workerList.size() == numberOfWorkers) {
      return workerList;
    }

    return getWorkerListFromJobMaster();
  }

  @Override
  public List<WorkerNetworkInfo> waitForAllWorkersToJoin(long timeLimit) {
    if (workerList.size() == numberOfWorkers) {
      return workerList;
    }

    sendWorkerListRequest(ListWorkersRequest.RequestType.RESPONSE_AFTER_ALL_JOINED);

    boolean timeLimitReached = JobMasterClient.loopUntil(responseReceived, timeLimit);
    if (timeLimitReached) {
      LOG.severe("Timelimit has been reached and the worker list has not been received "
          + "from the job master. ");
      return null;
    }
    return workerList;
  }

  private List<WorkerNetworkInfo> getWorkerListFromJobMaster() {

    sendWorkerListRequest(ListWorkersRequest.RequestType.IMMEDIATE_RESPONSE);

    JobMasterClient.loopUntil(responseReceived, 1000);
    return workerList;
  }

  public void sendWorkerListRequest(ListWorkersRequest.RequestType requestType) {
    ListWorkersRequest listRequest = ListWorkersRequest.newBuilder()
        .setWorkerID(thisWorker.getWorkerID())
        .setRequestType(requestType)
        .build();

    responseReceived.setFalse();
    RequestID requestID = rrClient.sendRequest(listRequest);

    if (requestID == null) {
      LOG.severe("When sending ListWorkers message, requestID returned null.");
    } else {
      LOG.info("ListWorkers message sent to the master: \n" + listRequest);
    }
  }

  @Override
  public void onMessage(RequestID id, int workerId, Message message) {
    if (message instanceof ListWorkersResponse) {
      LOG.info("ListWorkersResponse message received from the master: \n" + message);

      ListWorkersResponse listResponse = (ListWorkersResponse) message;
      List<ListWorkersResponse.WorkerNetworkInfo> receivedWorkerInfos =
          listResponse.getWorkersList();

      workerList.clear();
      workerList.add(thisWorker);

      for (ListWorkersResponse.WorkerNetworkInfo receivedWorkerInfo: receivedWorkerInfos) {

        // if received worker info belongs to this worker, do not add
        if (receivedWorkerInfo.getId() != thisWorker.getWorkerID()) {
          InetAddress ip = convertStringToIP(receivedWorkerInfo.getIp());
          WorkerNetworkInfo workerInfo =
              new WorkerNetworkInfo(ip, receivedWorkerInfo.getPort(), receivedWorkerInfo.getId());
          workerList.add(workerInfo);
        }
      }

      responseReceived.setTrue();

    } else {
      LOG.warning("Received message unrecognized. \n" + message);
    }

  }

  /**
   * covert the given string to ip address object
   * @param ipStr
   * @return
   */
  public static InetAddress convertStringToIP(String ipStr) {
    try {
      return InetAddress.getByName(ipStr);
    } catch (UnknownHostException e) {
      LOG.log(Level.SEVERE, "Can not convert the IP string to InetAddress: " + ipStr, e);
      throw new RuntimeException(e);
    }
  }

  public static void printWorkers(List<WorkerNetworkInfo> workers) {

    if (workers == null) {
      return;
    }

    StringBuffer buffer = new StringBuffer();
    buffer.append("Number of workers: " + workers.size() + "\n");
    int i = 0;
    for (WorkerNetworkInfo worker: workers) {
      buffer.append(String.format("%d: workerID[%d] %s\n",
          i++, worker.getWorkerID(), worker.getWorkerName()));
    }

    LOG.info(buffer.toString());
  }

}
