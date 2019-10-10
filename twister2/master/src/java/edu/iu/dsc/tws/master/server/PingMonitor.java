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
import java.util.logging.Logger;

import com.google.protobuf.Message;

import edu.iu.dsc.tws.api.net.request.MessageHandler;
import edu.iu.dsc.tws.api.net.request.RequestID;
import edu.iu.dsc.tws.common.net.tcp.request.RRServer;
import edu.iu.dsc.tws.master.dashclient.DashboardClient;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;

public class PingMonitor implements MessageHandler {
  private static final Logger LOG = Logger.getLogger(PingMonitor.class.getName());

  // keep the timing of last heartbeat messages
  private HashMap<Integer, Long> lastTimeStamps;

  private WorkerMonitor workerMonitor;
  private RRServer rrServer;
  private DashboardClient dashClient;

  public PingMonitor(WorkerMonitor workerMonitor,
                     RRServer rrServer,
                     DashboardClient dashClient) {

    this.workerMonitor = workerMonitor;
    this.rrServer = rrServer;
    this.dashClient = dashClient;
    lastTimeStamps = new HashMap<>();
  }

  @Override
  public void onMessage(RequestID id, int workerId, Message message) {

    if (!(message instanceof JobMasterAPI.Ping)) {
      LOG.warning("A message received on PingMonitor but it is not JobMasterAPI.Ping message."
          + "Something not right. Ignoring the message: " + message);
      return;
    }

    JobMasterAPI.Ping ping = (JobMasterAPI.Ping) message;

    if (ping.getWorkerID() >= workerMonitor.getNumberOfWorkers()) {
      LOG.warning("A ping message is received from a worker with ID: " + ping.getWorkerID()
          + " That is higher than the range for this job. Current number of workers: "
          + workerMonitor.getNumberOfWorkers() + " Ignoring the message. ");
    } else {
      lastTimeStamps.put(ping.getWorkerID(), System.currentTimeMillis());
      LOG.fine("Ping message received: \n" + ping);
    }

    JobMasterAPI.Ping pingResponse = JobMasterAPI.Ping.newBuilder()
        .setWorkerID(ping.getWorkerID())
        .setMessageType(JobMasterAPI.Ping.MessageType.MASTER_TO_WORKER)
        .build();

    rrServer.sendResponse(id, pingResponse);
    LOG.fine("Ping response sent to the worker: \n" + pingResponse);

    // send Ping message to dashboard
    if (dashClient != null) {
      dashClient.workerHeartbeat(ping.getWorkerID());
    }

  }
}
