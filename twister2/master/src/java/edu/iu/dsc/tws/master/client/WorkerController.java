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

import edu.iu.dsc.tws.common.discovery.IWorkerController;
import edu.iu.dsc.tws.common.discovery.WorkerNetworkInfo;
import edu.iu.dsc.tws.common.net.tcp.request.MessageHandler;
import edu.iu.dsc.tws.common.net.tcp.request.RRClient;
import edu.iu.dsc.tws.common.net.tcp.request.RequestID;
import edu.iu.dsc.tws.proto.network.Network.ListWorkersRequest;
import edu.iu.dsc.tws.proto.network.Network.ListWorkersResponse;

public class WorkerController implements IWorkerController, MessageHandler {
  private static final Logger LOG = Logger.getLogger(WorkerController.class.getName());

  private WorkerNetworkInfo thisWorker;
  private ArrayList<WorkerNetworkInfo> workerList;
  private int numberOfWorkers;

  private RRClient rrClient;

  // these variables are used to wait the response messages
  // when the worker list is requested from the job master
  private Object synchObject1 = new Object();
  private boolean waitingResponse1 = false;
  public static final long MAX_WAIT_TIME_FOR_IMMEDIATE_RESPONSE = 500;

  private Object synchObject2 = new Object();
  private boolean waitingResponse2 = false;

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
    for (WorkerNetworkInfo info : workerList) {
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

    boolean sent = sendWorkerListRequest(ListWorkersRequest.RequestType.RESPONSE_AFTER_ALL_JOINED);
    if (!sent) {
      return null;
    }

    synchronized (synchObject2) {
      try {
        waitingResponse2 = true;
        synchObject2.wait(timeLimit);
        // if waitingResponse2 is still true after waking up,
        // it means the timeLimit has been reached
        if (waitingResponse2) {
          waitingResponse2 = false;
          LOG.severe("Timelimit has been reached and the worker list has not been received "
              + "from the job master. ");
          return null;
        }

      } catch (InterruptedException e) {
        waitingResponse2 = false;
        LOG.warning("Waiting thread interrupted when waiting to get the response message. ");
        return null;
      }
    }
    return workerList;
  }

  private List<WorkerNetworkInfo> getWorkerListFromJobMaster() {

    boolean sent = sendWorkerListRequest(ListWorkersRequest.RequestType.IMMEDIATE_RESPONSE);
    if (!sent) {
      return null;
    }

    synchronized (synchObject1) {
      try {
        waitingResponse1 = true;
        synchObject1.wait(MAX_WAIT_TIME_FOR_IMMEDIATE_RESPONSE);
        // if waitingResponse1 is still true after waking up,
        // it means the timeLimit has been reached
        if (waitingResponse1) {
          waitingResponse1 = false;
          LOG.info("Timelimit has been reached and the worker list has not been received "
              + "from the job master. It will provide the list as is.");
          return workerList;
        }
      } catch (InterruptedException e) {
        waitingResponse1 = false;
        LOG.warning("Waiting thread interrupted when waiting to get response message. ");
        return null;
      }
    }

    return workerList;
  }

  public boolean sendWorkerListRequest(ListWorkersRequest.RequestType requestType) {
    ListWorkersRequest listRequest = ListWorkersRequest.newBuilder()
        .setWorkerID(thisWorker.getWorkerID())
        .setRequestType(requestType)
        .build();

    RequestID requestID = rrClient.sendRequest(listRequest);

    if (requestID == null) {
      LOG.severe("When sending ListWorkers message, requestID returned null.");
      return false;
    } else {
      LOG.info("ListWorkers message sent to the master: \n" + listRequest);
      return true;
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

      for (ListWorkersResponse.WorkerNetworkInfo receivedWorkerInfo : receivedWorkerInfos) {

        // if received worker info belongs to this worker, do not add
        if (receivedWorkerInfo.getId() != thisWorker.getWorkerID()) {
          InetAddress ip = convertStringToIP(receivedWorkerInfo.getIp());
          WorkerNetworkInfo workerInfo =
              new WorkerNetworkInfo(ip, receivedWorkerInfo.getPort(), receivedWorkerInfo.getId());
          workerList.add(workerInfo);
        }
      }

      if (waitingResponse1) {
        waitingResponse1 = false;
        synchronized (synchObject1) {
          synchObject1.notify();
        }
      }

      if (waitingResponse2) {
        waitingResponse2 = false;
        synchronized (synchObject2) {
          synchObject2.notify();
        }
      }

    } else {
      LOG.warning("Received message unrecognized. \n" + message);
    }

  }

  /**
   * convert the given string to ip address object
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
    for (WorkerNetworkInfo worker : workers) {
      buffer.append(String.format("%d: workerID[%d] %s\n",
          i++, worker.getWorkerID(), worker.getWorkerName()));
    }

    LOG.info(buffer.toString());
  }

}
