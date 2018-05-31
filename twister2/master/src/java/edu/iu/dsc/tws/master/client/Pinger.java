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

import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.Message;

import edu.iu.dsc.tws.common.net.tcp.request.MessageHandler;
import edu.iu.dsc.tws.common.net.tcp.request.RRClient;
import edu.iu.dsc.tws.common.net.tcp.request.RequestID;
import edu.iu.dsc.tws.proto.network.Network;

public class Pinger extends Thread implements MessageHandler {
  private static final Logger LOG = Logger.getLogger(Pinger.class.getName());

  private int workerID;
  private RRClient rrClient;
  private long interval;

  private RequestID requestID = null;
  private boolean stopRequested = false;

  public Pinger(int workerID, RRClient rrClient, long interval) {
    this.workerID = workerID;
    this.rrClient = rrClient;
    this.interval = interval;
  }

  public void stopPinger() {
    stopRequested = true;
    interrupt();
  }

  @Override
  public void run() {
    Network.Ping ping = Network.Ping.newBuilder()
        .setWorkerID(workerID)
        .setPingMessage("Ping Message From the Worker to the Job Master")
        .setMessageType(Network.Ping.MessageType.WORKER_TO_MASTER)
        .build();

    while (!stopRequested) {
      requestID = rrClient.sendRequest(ping);

      if (requestID == null) {
        LOG.severe("When sending Ping message, the requestID returned null.");
      } else {
        LOG.info("Ping request message sent to the master: \n" + ping);
      }

      try {
        sleep(interval);
      } catch (InterruptedException e) {
        if (!stopRequested) {
          LOG.log(Level.WARNING, "Pinger Thread sleep interrupted.", e);
        }
      }

    }
  }

  @Override
  public void onMessage(RequestID id, int workerId, Message message) {
    if (message instanceof Network.Ping) {
      LOG.info("Ping Response message received from the master: \n" + message);

      if (!requestID.equals(id)) {
        LOG.severe("Ping Response message requestID does not match.");
      }
    } else {
      LOG.warning("Received message unrecognized. \n" + message);
    }
  }
}
