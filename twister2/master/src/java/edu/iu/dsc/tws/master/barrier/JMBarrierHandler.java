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
package edu.iu.dsc.tws.master.barrier;

import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Logger;

import com.google.protobuf.Message;

import edu.iu.dsc.tws.api.net.request.MessageHandler;
import edu.iu.dsc.tws.api.net.request.RequestID;
import edu.iu.dsc.tws.common.net.tcp.request.RRServer;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;

public class JMBarrierHandler implements MessageHandler, BarrierResponder {
  private static final Logger LOG = Logger.getLogger(JMBarrierHandler.class.getName());

  private RRServer rrServer;
  private BarrierMonitor barrierMonitor;

  private TreeMap<Integer, RequestID> defaultWaitList;
  private TreeMap<Integer, RequestID> initWaitList;

  public JMBarrierHandler(RRServer rrServer, BarrierMonitor barrierMonitor) {
    this.rrServer = rrServer;
    this.barrierMonitor = barrierMonitor;

    defaultWaitList = new TreeMap<>();
    initWaitList = new TreeMap<>();

    JobMasterAPI.BarrierRequest.Builder barrierRequestBuilder =
        JobMasterAPI.BarrierRequest.newBuilder();
    JobMasterAPI.BarrierResponse.Builder barrierResponseBuilder =
        JobMasterAPI.BarrierResponse.newBuilder();

    rrServer.registerRequestHandler(barrierRequestBuilder, this);
    rrServer.registerRequestHandler(barrierResponseBuilder, this);
  }

  @Override
  public void onMessage(RequestID requestID, int workerId, Message message) {
    if (!(message instanceof JobMasterAPI.BarrierRequest)) {
      // this is not supposed to happen, so we are not sending out a response
      LOG.severe("Un-known message type received: " + message + " Ignoring this message.");
      return;
    }

    JobMasterAPI.BarrierRequest barrierRequest = (JobMasterAPI.BarrierRequest) message;

    if (barrierRequest.getBarrierType() == JobMasterAPI.BarrierType.DEFAULT) {

      defaultWaitList.put(barrierRequest.getWorkerID(), requestID);
      barrierMonitor.arrivedAtDefault(barrierRequest.getWorkerID(), barrierRequest.getTimeout());

    } else if (barrierRequest.getBarrierType() == JobMasterAPI.BarrierType.INIT) {

      initWaitList.put(barrierRequest.getWorkerID(), requestID);
      barrierMonitor.arrivedAtInit(barrierRequest.getWorkerID(), barrierRequest.getTimeout());

    } else {
      // this is not supposed to happen, so we are not sending out a response
      LOG.warning("Received barrier request with unrecognized type: "
          + barrierRequest.getBarrierType() + " Ignoring this event.");
    }
  }

  @Override
  public void allArrived(JobMasterAPI.BarrierType barrierType) {
    if (barrierType == JobMasterAPI.BarrierType.DEFAULT) {
      sendBarrierResponses(defaultWaitList, barrierType, JobMasterAPI.BarrierResult.SUCCESS);
      defaultWaitList.clear();
    } else if (barrierType == JobMasterAPI.BarrierType.INIT) {
      sendBarrierResponses(initWaitList, barrierType, JobMasterAPI.BarrierResult.SUCCESS);
      initWaitList.clear();
    }
  }

  @Override
  public void barrierFailed(JobMasterAPI.BarrierType barrierType,
                            JobMasterAPI.BarrierResult result) {

    if (barrierType == JobMasterAPI.BarrierType.DEFAULT) {
      sendBarrierResponses(defaultWaitList, barrierType, result);
      defaultWaitList.clear();
    } else if (barrierType == JobMasterAPI.BarrierType.INIT) {
      sendBarrierResponses(initWaitList, barrierType, result);
      initWaitList.clear();
    }
  }

  /**
   * send success response messages to all workers in waitList
   */
  private void sendBarrierResponses(Map<Integer, RequestID> waitList,
                                    JobMasterAPI.BarrierType barrierType,
                                    JobMasterAPI.BarrierResult result) {

    for (Map.Entry<Integer, RequestID> entry : waitList.entrySet()) {
      JobMasterAPI.BarrierResponse response = JobMasterAPI.BarrierResponse.newBuilder()
          .setWorkerID(entry.getKey())
          .setBarrierType(barrierType)
          .setResult(result)
          .build();

      rrServer.sendResponse(entry.getValue(), response);
      LOG.fine("BarrierResponse message sent:\n" + response);
    }
  }

  /**
   * send failed response message to a worker
   */
  private void sendFailResponse(RequestID requestID,
                                int workerID,
                                JobMasterAPI.BarrierType barrierType,
                                JobMasterAPI.BarrierResult result) {

    JobMasterAPI.BarrierResponse response = JobMasterAPI.BarrierResponse.newBuilder()
        .setWorkerID(workerID)
        .setBarrierType(barrierType)
        .setResult(result)
        .build();

    rrServer.sendResponse(requestID, response);
    LOG.fine("Sending failed BarrierResponse message:\n" + response);
  }

}
