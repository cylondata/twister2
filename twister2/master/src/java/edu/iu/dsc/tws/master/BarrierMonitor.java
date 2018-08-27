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
package edu.iu.dsc.tws.master;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.Message;

import edu.iu.dsc.tws.common.net.tcp.request.MessageHandler;
import edu.iu.dsc.tws.common.net.tcp.request.RRServer;
import edu.iu.dsc.tws.common.net.tcp.request.RequestID;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;

public class BarrierMonitor implements MessageHandler {
  private static final Logger LOG = Logger.getLogger(BarrierMonitor.class.getName());

  private int numberOfWorkers;
  private HashMap<Integer, RequestID> waitList;
  private RRServer rrServer;

  public BarrierMonitor(int numberOfWorkers, RRServer rrServer) {
    this.numberOfWorkers = numberOfWorkers;
    this.rrServer = rrServer;
    waitList = new HashMap<>();
  }

  @Override
  public void onMessage(RequestID requestID, int workerId, Message message) {

    if (message instanceof JobMasterAPI.BarrierRequest) {
      JobMasterAPI.BarrierRequest barrierRequest = (JobMasterAPI.BarrierRequest) message;
      LOG.info("BarrierRequest message received:\n" + barrierRequest);

      waitList.put(barrierRequest.getWorkerID(), requestID);

      if (waitList.size() == numberOfWorkers) {
        sendBarrierResponseToWaitList();
      }

    } else {
      LOG.log(Level.SEVERE, "Un-known message received: " + message);
    }
  }

  /**
   * send the response messages to all workers in waitList
   */
  private void sendBarrierResponseToWaitList() {

    for (Map.Entry<Integer, RequestID> entry: waitList.entrySet()) {
      JobMasterAPI.BarrierResponse response = JobMasterAPI.BarrierResponse.newBuilder()
          .setWorkerID(entry.getKey())
          .build();

      rrServer.sendResponse(entry.getValue(), response);
      LOG.info("BarrierResponse sent:\n" + response);
    }

    waitList.clear();
  }
}
