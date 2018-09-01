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
import edu.iu.dsc.tws.common.discovery.NodeInfo;
import edu.iu.dsc.tws.common.discovery.WorkerNetworkInfo;
import edu.iu.dsc.tws.common.net.tcp.request.BlockingSendException;
import edu.iu.dsc.tws.common.net.tcp.request.MessageHandler;
import edu.iu.dsc.tws.common.net.tcp.request.RRClient;
import edu.iu.dsc.tws.common.net.tcp.request.RequestID;
import edu.iu.dsc.tws.master.JobMasterContext;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI.ListWorkersRequest;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI.ListWorkersResponse;

public class JMWorkerController implements IWorkerController, MessageHandler {
  private static final Logger LOG = Logger.getLogger(JMWorkerController.class.getName());

  private WorkerNetworkInfo thisWorker;
  private ArrayList<WorkerNetworkInfo> workerList;
  private int numberOfWorkers;

  private RRClient rrClient;
  private Config config;

  public JMWorkerController(Config config, WorkerNetworkInfo thisWorker, RRClient rrClient) {
    this(config, thisWorker, rrClient, JobMasterContext.workerInstances(config));
  }

  public JMWorkerController(Config config, WorkerNetworkInfo thisWorker,
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

    LOG.info("Sending ListWorkers message to the master: \n" + listRequest);
    try {
      rrClient.sendRequestWaitResponse(listRequest, timeLimit);
      return true;

    } catch (BlockingSendException e) {
      LOG.log(Level.SEVERE, e.getMessage(), e);
      return false;
    }

  }

  @Override
  public void onMessage(RequestID id, int workerId, Message message) {
    if (message instanceof ListWorkersResponse) {
      LOG.info("ListWorkersResponse message received from the master: \n" + message);

      ListWorkersResponse listResponse = (ListWorkersResponse) message;
      List<JobMasterAPI.WorkerNetworkInfo> receivedWorkerInfos =
          listResponse.getWorkersList();

      workerList.clear();
      workerList.add(thisWorker);

      for (JobMasterAPI.WorkerNetworkInfo receivedWorkerInfo : receivedWorkerInfos) {

        // if received worker info belongs to this worker, do not add
        if (receivedWorkerInfo.getWorkerID() != thisWorker.getWorkerID()) {
          NodeInfo nodeInfo = new NodeInfo(receivedWorkerInfo.getNodeIP(),
              receivedWorkerInfo.getRackName(),
              receivedWorkerInfo.getDataCenterName());

          InetAddress ip = convertStringToIP(receivedWorkerInfo.getWorkerIP());

          WorkerNetworkInfo workerInfo = new WorkerNetworkInfo(
              ip, receivedWorkerInfo.getPort(), receivedWorkerInfo.getWorkerID(), nodeInfo);

          workerList.add(workerInfo);
        }
      }

    } else if (message instanceof JobMasterAPI.BarrierResponse) {
      LOG.info("Received a BarrierResponse message from the master. \n" + message);

    } else {
      LOG.warning("Received message unrecognized. \n" + message);
    }

  }

  public boolean waitOnBarrier(long timeLimitMilliSec) {

    JobMasterAPI.BarrierRequest barrierRequest = JobMasterAPI.BarrierRequest.newBuilder()
        .setWorkerID(thisWorker.getWorkerID())
        .build();

    LOG.info("Sending BarrierRequest message: \n" + barrierRequest.toString());
    try {
      rrClient.sendRequestWaitResponse(barrierRequest, timeLimitMilliSec);
      return true;
    } catch (BlockingSendException e) {
      LOG.log(Level.SEVERE, e.getMessage(), e);
      return false;
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

}
