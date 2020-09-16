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

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

import edu.iu.dsc.tws.api.net.request.MessageHandler;
import edu.iu.dsc.tws.api.net.request.RequestID;
import edu.iu.dsc.tws.api.resource.IReceiverFromDriver;
import edu.iu.dsc.tws.api.resource.ISenderToDriver;
import edu.iu.dsc.tws.common.net.tcp.request.RRClient;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;

public class JMDriverAgent implements ISenderToDriver, MessageHandler {
  private static final Logger LOG = Logger.getLogger(JMDriverAgent.class.getName());

  private RRClient rrClient;
  private int workerID;

  /**
   * workers can register an IReceiverFromDriver
   * to receive messages from the driver
   */
  private IReceiverFromDriver receiverFromDriver;

  /**
   * if messages arrive before IReceiverFromDriver added,
   * buffer those messages in this list and deliver them when an IReceiverFromDriver added
   */
  private LinkedList<JobMasterAPI.DriverMessage> messageBuffer = new LinkedList<>();

  public JMDriverAgent(RRClient rrClient, int workerID) {
    this.rrClient = rrClient;
    this.workerID = workerID;

    rrClient.registerResponseHandler(JobMasterAPI.DriverMessage.newBuilder(), this);
    rrClient.registerResponseHandler(JobMasterAPI.WorkerMessage.newBuilder(), this);
    rrClient.registerResponseHandler(JobMasterAPI.WorkerMessageResponse.newBuilder(), this);
  }

  /**
   * only one IReceiverFromDriver can be added
   * if the second IReceiverFromDriver tried to be added, returns false
   */
  public boolean addReceiverFromDriver(IReceiverFromDriver receiverFromDriver1) {
    if (receiverFromDriver != null) {
      return false;
    }

    this.receiverFromDriver = receiverFromDriver1;

    // deliver buffered messages if any
    deliverBufferedMessages();

    return true;
  }

  @Override
  public boolean sendToDriver(Message message) {
    JobMasterAPI.WorkerMessage workerMessage = JobMasterAPI.WorkerMessage.newBuilder()
        .setData(Any.pack(message).toByteString())
        .setWorkerID(workerID)
        .build();

    RequestID requestID = rrClient.sendRequest(workerMessage);
    if (requestID == null) {
      LOG.severe("Could not send WorkerToDriver message.");
      return false;
    }

    LOG.fine("Sent WorkerToDriver message: \n" + workerMessage);
    return true;
  }

  @Override
  public void onMessage(RequestID id, int workerId, Message message) {
    if (message instanceof JobMasterAPI.WorkerMessageResponse) {
      LOG.fine("Received a WorkerMessageResponse from the master. \n" + message);

    } else if (message instanceof JobMasterAPI.DriverMessage) {

      JobMasterAPI.DriverMessage driverMessage = (JobMasterAPI.DriverMessage) message;
      if (receiverFromDriver == null) {
        messageBuffer.add(driverMessage);
      } else {
        deliverMessageToReceiver(driverMessage);
      }

    }
  }

  /**
   * deliver all buffered messages to the IReceiverFromDriver
   */
  private void deliverBufferedMessages() {

    while (!messageBuffer.isEmpty()) {
      deliverMessageToReceiver(messageBuffer.poll());
    }
  }

  /**
   * deliver the received message to IReceiverFromDriver
   */
  private void deliverMessageToReceiver(JobMasterAPI.DriverMessage message) {

    try {
      Any any = Any.parseFrom(message.getData());
      receiverFromDriver.driverMessageReceived(any);
    } catch (InvalidProtocolBufferException e) {
      LOG.log(Level.SEVERE, "Can not parse received protocol buffer message to Any", e);
    }
  }

}
