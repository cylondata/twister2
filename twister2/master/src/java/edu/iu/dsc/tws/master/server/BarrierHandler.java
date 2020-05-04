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

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.Message;

import edu.iu.dsc.tws.api.net.request.MessageHandler;
import edu.iu.dsc.tws.api.net.request.RequestID;
import edu.iu.dsc.tws.api.resource.IBarrierListener;
import edu.iu.dsc.tws.common.net.tcp.request.RRServer;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;

/**
 * all workers in a job may wait on a barrier point
 * BarrierHandler will get barrier requests from all workers
 * It will send response message to all workers,
 * when it gets the request from the last worker in a job
 */

public class BarrierHandler implements MessageHandler {
  private static final Logger LOG = Logger.getLogger(BarrierHandler.class.getName());

  private WorkerMonitor workerMonitor;
  private int numberOfWorkersOnDefaultBarrier;
  private int numberOfWorkersOnInitBarrier;
  private HashMap<Integer, RequestID> defaultWaitList;
  private HashMap<Integer, RequestID> initWaitList;
  private RRServer rrServer;
  private IBarrierListener barrierListener;

  public BarrierHandler(WorkerMonitor workerMonitor, RRServer rrServer, IBarrierListener bl) {
    this.workerMonitor = workerMonitor;
    this.rrServer = rrServer;
    this.barrierListener = bl;
    defaultWaitList = new HashMap<>();
    initWaitList = new HashMap<>();
  }

  @Override
  public void onMessage(RequestID requestID, int workerId, Message message) {

    if (message instanceof JobMasterAPI.BarrierRequest) {
      JobMasterAPI.BarrierRequest barrierRequest = (JobMasterAPI.BarrierRequest) message;

      if (barrierRequest.getBarrierType() == JobMasterAPI.BarrierType.DEFAULT) {
        receivedDefaultBarrierRequest(requestID, barrierRequest);
      } else if (barrierRequest.getBarrierType() == JobMasterAPI.BarrierType.INIT) {
        receivedInitBarrierRequest(requestID, barrierRequest);
      } else {
        LOG.warning("Received barrier request with unrecognized type: "
            + barrierRequest.getBarrierType() + " Sending fail response.");
        sendFailResponse(requestID, barrierRequest.getWorkerID(), barrierRequest.getBarrierType());
      }

    } else {
      LOG.log(Level.SEVERE, "Un-known message received: " + message);
    }
  }

  public void receivedDefaultBarrierRequest(RequestID requestID,
                                            JobMasterAPI.BarrierRequest message) {

    // log first and last workers messages as INFO, others as FINE
    // numberOfWorkers in a job may change during job execution
    // numberOfWorkersOnBarrier is assigned the value of numberOfWorkers in the job
    // when the first barrier message received
    if (defaultWaitList.size() == 0) {
      numberOfWorkersOnDefaultBarrier = workerMonitor.getNumberOfWorkers();
      LOG.fine("First Default BarrierRequest message received:\n" + message);
    } else {
      LOG.fine("BarrierRequest message received:\n" + message);
    }

    defaultWaitList.put(message.getWorkerID(), requestID);

    // if all workers arrived at the barrier
    if (defaultWaitList.size() == numberOfWorkersOnDefaultBarrier) {

      // send response messages to all workers
      LOG.info("All workers reached the default barrier: " + numberOfWorkersOnDefaultBarrier
          + " Sending out BarrierResponse messages to all workers.");
      sendBarrierResponseToWaitList(defaultWaitList, message.getBarrierType());

      // clear wait list for the next barrier event
      defaultWaitList.clear();
      numberOfWorkersOnDefaultBarrier = 0;
    }
  }

  public void receivedInitBarrierRequest(RequestID requestID, JobMasterAPI.BarrierRequest message) {

    // log first and last workers messages as INFO, others as FINE
    // numberOfWorkers in a job may change during job execution
    // numberOfWorkersOnBarrier is assigned the value of numberOfWorkers in the job
    // when the first barrier message received
    if (initWaitList.size() == 0) {
      numberOfWorkersOnInitBarrier = workerMonitor.getNumberOfWorkers();
      LOG.fine("First Init BarrierRequest message received:\n" + message);
    } else {
      LOG.fine("BarrierRequest message received:\n" + message);
    }

    initWaitList.put(message.getWorkerID(), requestID);

    // if all workers arrived at the barrier
    if (initWaitList.size() == numberOfWorkersOnInitBarrier) {

      // first let the barrier listener know
      if (barrierListener != null) {
        barrierListener.allArrived();
      }

      // send response messages to all workers
      LOG.info("All workers reached the init barrier: " + numberOfWorkersOnInitBarrier
          + " Sending out BarrierResponse messages to all workers.");
      sendBarrierResponseToWaitList(initWaitList, message.getBarrierType());

      // clear wait list for the next barrier event
      initWaitList.clear();
      numberOfWorkersOnInitBarrier = 0;
    }
  }

  /**
   * send the response messages to all workers in waitList
   */
  private void sendBarrierResponseToWaitList(HashMap<Integer, RequestID> waitList,
                                             JobMasterAPI.BarrierType barrierType) {

    for (Map.Entry<Integer, RequestID> entry : waitList.entrySet()) {
      JobMasterAPI.BarrierResponse response = JobMasterAPI.BarrierResponse.newBuilder()
          .setWorkerID(entry.getKey())
          .setBarrierType(barrierType)
          .setSucceeded(true)
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
                                JobMasterAPI.BarrierType barrierType) {

    JobMasterAPI.BarrierResponse response = JobMasterAPI.BarrierResponse.newBuilder()
        .setWorkerID(workerID)
        .setBarrierType(barrierType)
        .setSucceeded(false)
        .build();

    rrServer.sendResponse(requestID, response);
    LOG.fine("Sending failed BarrierResponse message:\n" + response);
  }
}
