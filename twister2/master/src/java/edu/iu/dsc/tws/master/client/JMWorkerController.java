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
import edu.iu.dsc.tws.common.controller.ControllerContext;
import edu.iu.dsc.tws.common.controller.IWorkerController;
import edu.iu.dsc.tws.common.exceptions.TimeoutException;
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

  private JobMasterAPI.WorkerInfo thisWorker;
  private ArrayList<JobMasterAPI.WorkerInfo> workerList;
  private int numberOfWorkers;

  private RRClient rrClient;
  private Config config;

  public JMWorkerController(Config config, JobMasterAPI.WorkerInfo thisWorker, RRClient rrClient) {
    this(config, thisWorker, rrClient, JobMasterContext.workerInstances(config));
  }

  public JMWorkerController(Config config, JobMasterAPI.WorkerInfo thisWorker,
                            RRClient rrClient, int numberOfWorkers) {
    this.config = config;
    this.numberOfWorkers = numberOfWorkers;
    this.thisWorker = thisWorker;
    this.rrClient = rrClient;
    workerList = new ArrayList<>();
    workerList.add(thisWorker);
  }

  @Override
  public JobMasterAPI.WorkerInfo getWorkerInfo() {
    return thisWorker;
  }

  @Override
  public JobMasterAPI.WorkerInfo getWorkerInfoForID(int id) {
    for (JobMasterAPI.WorkerInfo info : workerList) {
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
  public List<JobMasterAPI.WorkerInfo> getJoinedWorkers() {

    if (workerList.size() == numberOfWorkers) {
      return workerList;
    }

    // get the worker list from the job master
    sendWorkerListRequest(ListWorkersRequest.RequestType.IMMEDIATE_RESPONSE,
        JobMasterContext.responseWaitDuration(config));

    return workerList;
  }

  @Override
  public List<JobMasterAPI.WorkerInfo> getAllWorkers() throws TimeoutException {
    if (workerList.size() == numberOfWorkers) {
      return workerList;
    }

    long timeLimit = ControllerContext.maxWaitTimeForAllToJoin(config);
    boolean sentAndReceived =
        sendWorkerListRequest(ListWorkersRequest.RequestType.RESPONSE_AFTER_ALL_JOINED, timeLimit);

    if (!sentAndReceived) {
      throw
          new TimeoutException("All workers have not joined the job on the specified time limit: "
          + timeLimit + "ms.");
    }

    return workerList;
  }

  private boolean sendWorkerListRequest(ListWorkersRequest.RequestType requestType,
                                        long timeLimit) {

    ListWorkersRequest listRequest = ListWorkersRequest.newBuilder()
        .setWorkerID(thisWorker.getWorkerID())
        .setRequestType(requestType)
        .build();

    LOG.fine("Sending ListWorkers message to the master: \n" + listRequest);
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
      LOG.fine("ListWorkersResponse message received from the master: \n" + message);

      ListWorkersResponse listResponse = (ListWorkersResponse) message;
      List<JobMasterAPI.WorkerInfo> receivedWorkerInfos =
          listResponse.getWorkerList();

      workerList.clear();
      workerList.add(thisWorker);

      for (JobMasterAPI.WorkerInfo receivedWorkerInfo : receivedWorkerInfos) {

        // if received worker info belongs to this worker, do not add
        if (receivedWorkerInfo.getWorkerID() != thisWorker.getWorkerID()) {
          workerList.add(receivedWorkerInfo);
        }
      }

    } else if (message instanceof JobMasterAPI.BarrierResponse) {
      LOG.fine("Received a BarrierResponse message from the master. \n" + message);

    } else {
      LOG.warning("Received message unrecognized. \n" + message);
    }

  }

  public void waitOnBarrier() throws TimeoutException {

    JobMasterAPI.BarrierRequest barrierRequest = JobMasterAPI.BarrierRequest.newBuilder()
        .setWorkerID(thisWorker.getWorkerID())
        .build();

    LOG.fine("Sending BarrierRequest message: \n" + barrierRequest.toString());
    try {
      rrClient.sendRequestWaitResponse(barrierRequest,
          ControllerContext.maxWaitTimeOnBarrier(config));
    } catch (BlockingSendException e) {
      throw new TimeoutException("All workers have not arrived at the barrier on the time limit: "
          + ControllerContext.maxWaitTimeOnBarrier(config) + "ms.", e);
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
