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

import java.util.logging.Logger;

import com.google.protobuf.Message;

import edu.iu.dsc.tws.api.net.request.MessageHandler;
import edu.iu.dsc.tws.api.net.request.RequestID;
import edu.iu.dsc.tws.common.net.tcp.request.RRServer;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;

/**
 * Handles JobMaster to worker messaging
 */

public class WorkerHandler implements MessageHandler {

  private static final Logger LOG = Logger.getLogger(WorkerHandler.class.getName());

  private WorkerMonitor workerMonitor;
  private RRServer rrServer;

  public WorkerHandler(WorkerMonitor workerMonitor, RRServer rrServer) {
    this.workerMonitor = workerMonitor;
    this.rrServer = rrServer;
  }

  @Override
  public void onMessage(RequestID id, int workerId, Message message) {

    if (message instanceof JobMasterAPI.RegisterWorker) {
      JobMasterAPI.RegisterWorker rwMessage = (JobMasterAPI.RegisterWorker) message;
      registerWorkerMessageReceived(id, rwMessage);
    }


  }

  private void registerWorkerMessageReceived(RequestID id, JobMasterAPI.RegisterWorker message) {

    LOG.fine("RegisterWorker message received: \n" + message);
    JobMasterAPI.WorkerInfo workerInfo = message.getWorkerInfo();
    JobMasterAPI.WorkerState initialState = JobMasterAPI.WorkerState.STARTING;

    if (message.getFromFailure()) {
      initialState = JobMasterAPI.WorkerState.RESTARTING;
    }

    String failMessage = workerMonitor.joinWorker(workerInfo, initialState);

    if (failMessage == null) {
      // send a success response
      sendRegisterWorkerResponse(id, workerInfo.getWorkerID(), true, null);
    } else {
      // send a failure response
      sendRegisterWorkerResponse(id, workerInfo.getWorkerID(), false, failMessage);
    }

  }

  private void sendRegisterWorkerResponse(RequestID id,
                                          int workerID,
                                          boolean result,
                                          String reason) {

    JobMasterAPI.RegisterWorkerResponse response =
        JobMasterAPI.RegisterWorkerResponse.newBuilder()
            .setWorkerID(workerID)
            .setResult(result)
            .setReason(reason == null ? "" : reason)
            .build();

    rrServer.sendResponse(id, response);
    LOG.fine("RegisterWorkerResponse sent:\n" + response);
  }

}
