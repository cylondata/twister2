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

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.discovery.IWorkerController;
import edu.iu.dsc.tws.common.discovery.WorkerNetworkInfo;
import edu.iu.dsc.tws.common.net.tcp.request.MessageHandler;
import edu.iu.dsc.tws.common.net.tcp.request.RRClient;
import edu.iu.dsc.tws.common.net.tcp.request.RequestID;
import edu.iu.dsc.tws.master.JobMasterContext;
import edu.iu.dsc.tws.proto.network.Network;
import edu.iu.dsc.tws.proto.network.Network.ListWorkersRequest;
import edu.iu.dsc.tws.proto.network.Network.ListWorkersResponse;

public class WorkerController implements IWorkerController, MessageHandler {
  private static final Logger LOG = Logger.getLogger(WorkerController.class.getName());

  private WorkerNetworkInfo thisWorker;
  private ArrayList<WorkerNetworkInfo> workerList;
  private int numberOfWorkers;

  private RRClient rrClient;
  private Config config;

  public WorkerController(Config config, WorkerNetworkInfo thisWorker, RRClient rrClient) {
    this(config, thisWorker, rrClient, JobMasterContext.workerInstances(config));
  }

  public WorkerController(Config config, WorkerNetworkInfo thisWorker,
                          RRClient rrClient, int numberOfWorkers) {
    this.config = config;
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

    // get the worker list from the job master
    sendWorkerListRequest(ListWorkersRequest.RequestType.IMMEDIATE_RESPONSE,
        JobMasterContext.responseWaitDuration(config));

    return workerList;
  }

  @Override
  public List<WorkerNetworkInfo> waitForAllWorkersToJoin(long timeLimitMilliSec) {
    if (workerList.size() == numberOfWorkers) {
      return workerList;
    }

    boolean sentAndReceived =
        sendWorkerListRequest(ListWorkersRequest.RequestType.RESPONSE_AFTER_ALL_JOINED,
            timeLimitMilliSec);

    if (!sentAndReceived) {
      return null;
    }

    return workerList;
  }

  private boolean sendWorkerListRequest(ListWorkersRequest.RequestType requestType,
                                        long timeLimit) {

    ListWorkersRequest listRequest = ListWorkersRequest.newBuilder()
        .setWorkerID(thisWorker.getWorkerID())
        .setRequestType(requestType)
        .build();

    RequestID requestID = rrClient.sendRequestWaitResponse(listRequest, timeLimit);

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

    } else if (message instanceof Network.BarrierResponse) {
      LOG.info("Received a BarrierResponse message from the master. \n" + message);

    } else {
      LOG.warning("Received message unrecognized. \n" + message);
    }

  }

  public boolean waitOnBarrier(long timeLimitMilliSec) {

    Network.BarrierRequest barrierRequest = Network.BarrierRequest.newBuilder()
        .setWorkerID(thisWorker.getWorkerID())
        .build();

    LOG.info("Sending BarrierRequest message: \n" + barrierRequest);
    RequestID requestID = rrClient.sendRequestWaitResponse(barrierRequest, timeLimitMilliSec);

    if (requestID == null) {
      LOG.severe("Couldn't send BarrierRequest message or couldn't receive the response.");
      return false;
    }

    return true;
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

}
