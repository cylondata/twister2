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

import java.util.logging.Logger;

import com.google.protobuf.Message;

import edu.iu.dsc.tws.api.net.request.MessageHandler;
import edu.iu.dsc.tws.api.net.request.RequestID;
import edu.iu.dsc.tws.common.net.tcp.request.RRClient;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;

public class Pinger implements MessageHandler {
  private static final Logger LOG = Logger.getLogger(Pinger.class.getName());

  private int workerID;
  private RRClient rrClient;
  private long interval;

  // shows the timestamp of the last ping message send time
  private long lastPingTime = -1;

  private RequestID requestID = null;

  private boolean pendingResponse;

  public Pinger(int workerID, RRClient rrClient, long interval) {
    this.workerID = workerID;
    this.rrClient = rrClient;
    this.interval = interval;
  }

  public long timeToNextPing() {

    if (lastPingTime == -1) {
      return interval;
    }

    long nextPingTime = lastPingTime + interval;
    return nextPingTime - System.currentTimeMillis();
  }

  public void sendPingMessage() {
    if (pendingResponse) {
      long timeSinceLast = System.currentTimeMillis() - lastPingTime;
      if (timeSinceLast > interval * 3) {
        LOG.warning(String.format("%d has been waiting %d milliseconds for a ping response "
                + "form JobMaster. Worker has already skipped %d ping requests "
                + "since the last successful ping.",
            workerID, timeSinceLast, timeSinceLast / interval));
      }
      return;
    }
    lastPingTime = System.currentTimeMillis();

    JobMasterAPI.Ping ping = JobMasterAPI.Ping.newBuilder()
        .setWorkerID(workerID)
        .setMessageType(JobMasterAPI.Ping.MessageType.WORKER_TO_MASTER)
        .build();

    requestID = rrClient.sendRequest(ping);
    LOG.fine("Sending Ping message: \n" + ping);
    this.pendingResponse = true;

    if (requestID == null) {
      LOG.severe("When sending Ping message, the requestID returned null.");
    }
  }

  @Override
  public void onMessage(RequestID id, int workerId, Message message) {
    if (message instanceof JobMasterAPI.Ping) {
      this.pendingResponse = false;

      if (!requestID.equals(id)) {
        LOG.severe("Ping Response message requestID does not match.");
      } else {
        LOG.fine("Ping Response message received: \n" + message);
      }
    } else {
      LOG.warning("Received message unrecognized. \n" + message);
    }
  }
}
