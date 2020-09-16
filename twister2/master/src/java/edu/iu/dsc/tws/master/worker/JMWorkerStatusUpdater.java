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
package edu.iu.dsc.tws.master.worker;

import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.Message;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.exceptions.net.BlockingSendException;
import edu.iu.dsc.tws.api.net.request.MessageHandler;
import edu.iu.dsc.tws.api.net.request.RequestID;
import edu.iu.dsc.tws.api.resource.IWorkerFailureListener;
import edu.iu.dsc.tws.api.resource.IWorkerStatusUpdater;
import edu.iu.dsc.tws.common.net.tcp.request.RRClient;
import edu.iu.dsc.tws.master.JobMasterContext;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;

public class JMWorkerStatusUpdater implements IWorkerStatusUpdater, MessageHandler {
  private static final Logger LOG = Logger.getLogger(JMWorkerController.class.getName());

  private RRClient rrClient;
  private int workerID;
  private Config config;

  private IWorkerFailureListener workerFailureListener;

  /**
   * if messages arrived before IWorkerFailureListener added,
   * buffer those messages in this list and deliver them when an IWorkerFailureListener added
   */
  private LinkedList<Message> messageBuffer = new LinkedList<>();

  public JMWorkerStatusUpdater(RRClient rrClient, int workerID, Config config) {
    this.rrClient = rrClient;
    this.workerID = workerID;
    this.config = config;

    rrClient.registerResponseHandler(JobMasterAPI.WorkerFailed.newBuilder(), this);
    rrClient.registerResponseHandler(JobMasterAPI.WorkerRestarted.newBuilder(), this);

    rrClient.registerResponseHandler(JobMasterAPI.WorkerStateChange.newBuilder(), this);
    rrClient.registerResponseHandler(JobMasterAPI.WorkerStateChangeResponse.newBuilder(), this);
  }

  @Override
  public boolean updateWorkerStatus(JobMasterAPI.WorkerState newState) {

    if (newState == JobMasterAPI.WorkerState.COMPLETED
        || newState == JobMasterAPI.WorkerState.KILLED
        || newState == JobMasterAPI.WorkerState.FAILED
        || newState == JobMasterAPI.WorkerState.FULLY_FAILED) {

      JobMasterAPI.WorkerStateChange workerStateChange = JobMasterAPI.WorkerStateChange.newBuilder()
          .setWorkerID(workerID)
          .setState(newState)
          .build();

      LOG.fine("Sending the message: \n" + workerStateChange);
      try {
        rrClient.sendRequestWaitResponse(workerStateChange,
            JobMasterContext.responseWaitDuration(config));
      } catch (BlockingSendException e) {
        LOG.log(Level.SEVERE, String.format("%d Worker completed message failed", workerID), e);
        return false;
      }

      return true;
    }

    LOG.severe("Unsupported state: " + newState);
    return false;
  }

  /**
   * only one IWorkerFailureListener can be added
   * if the second IWorkerFailureListener tried to be added, returns false
   */
  public boolean addWorkerFailureListener(IWorkerFailureListener workerFailureListener1) {
    if (this.workerFailureListener != null) {
      return false;
    }

    this.workerFailureListener = workerFailureListener1;

    // deliver buffered messages if any
    deliverBufferedMessages();

    return true;
  }


  @Override
  public JobMasterAPI.WorkerState getWorkerStatusForID(int id) {
    return null;
  }

  @Override
  public void onMessage(RequestID id, int workerId, Message message) {
    if (message instanceof JobMasterAPI.WorkerStateChangeResponse) {
      LOG.fine("Received a WorkerStateChange response from the master. \n" + message);
    } else if (message instanceof JobMasterAPI.WorkerFailed) {
      if (workerFailureListener == null) {
        messageBuffer.add(message);
      } else {
        JobMasterAPI.WorkerFailed workerFailed = (JobMasterAPI.WorkerFailed) message;
        workerFailureListener.failed(workerFailed.getWorkerID());
      }

    } else if (message instanceof JobMasterAPI.WorkerRestarted) {
      if (workerFailureListener == null) {
        messageBuffer.add(message);
      } else {
        JobMasterAPI.WorkerRestarted workerRestarted = (JobMasterAPI.WorkerRestarted) message;
        workerFailureListener.restarted(workerRestarted.getWorkerInfo());
      }
    }
  }

  /**
   * deliver all buffered messages to the IWorkerFailureListener
   */
  private void deliverBufferedMessages() {

    while (!messageBuffer.isEmpty()) {
      Message message = messageBuffer.poll();
      if (message instanceof JobMasterAPI.WorkerFailed) {
        JobMasterAPI.WorkerFailed workerFailed = (JobMasterAPI.WorkerFailed) message;
        workerFailureListener.failed(workerFailed.getWorkerID());
      } else if (message instanceof JobMasterAPI.WorkerRestarted) {
        JobMasterAPI.WorkerRestarted workerRestarted = (JobMasterAPI.WorkerRestarted) message;
        workerFailureListener.restarted(workerRestarted.getWorkerInfo());
      }
    }
  }

}
