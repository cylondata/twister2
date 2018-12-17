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

import java.util.logging.Logger;

import com.google.protobuf.Message;

import edu.iu.dsc.tws.common.net.tcp.request.MessageHandler;
import edu.iu.dsc.tws.common.net.tcp.request.RRServer;
import edu.iu.dsc.tws.common.net.tcp.request.RequestID;
import edu.iu.dsc.tws.master.server.JobMaster;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;

public class HTGClientMonitor implements MessageHandler {

  private static final Logger LOG = Logger.getLogger(HTGClientMonitor.class.getName());

  private JobMaster jobMaster;
  private RRServer rrServer;

  public HTGClientMonitor(JobMaster jobMaster, RRServer rrServer) {
    this.jobMaster = jobMaster;
    this.rrServer = rrServer;
  }

  @Override
  public void onMessage(RequestID id, int clientId, Message message) {

    LOG.info("Message received from the HTGClient:" + clientId + "\t" + message);

    if (message instanceof JobMasterAPI.HTGJobRequest) {
      JobMasterAPI.HTGJobRequest wscMessage = (JobMasterAPI.HTGJobRequest) message;
      stateChangeMessageReceived(id, wscMessage);
    }
  }

  private void stateChangeMessageReceived(RequestID id, JobMasterAPI.HTGJobRequest wscMessage) {

    JobMasterAPI.HTGJobResponse htgJobResponse = JobMasterAPI.HTGJobResponse.newBuilder()
        .setHtgSubgraphname(wscMessage.getExecuteMessage() + "finished")
        .build();

    rrServer.sendResponse(id, htgJobResponse);
    LOG.info("HTGClient response sent to the HTGClient: \n" + htgJobResponse);
  }
}
