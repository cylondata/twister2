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
package edu.iu.dsc.tws.api.htgjob;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.Any;
import com.google.protobuf.InvalidProtocolBufferException;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.driver.IDriver;
import edu.iu.dsc.tws.common.driver.IDriverMessenger;
import edu.iu.dsc.tws.common.driver.IScaler;
import edu.iu.dsc.tws.common.driver.WorkerListener;
import edu.iu.dsc.tws.master.driver.JMDriverAgent;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;
import edu.iu.dsc.tws.proto.system.job.HTGJobAPI;
import edu.iu.dsc.tws.proto.system.job.JobAPI;

public class Twister2HTGDriver implements IDriver, WorkerListener {

  private static final Logger LOG = Logger.getLogger(Twister2HTGDriver.class.getName());

  @Override
  public void execute(Config config, IScaler scaler, IDriverMessenger messenger) {

    LOG.info("%%%%%%%%% HTG JOB OBJECT:%%%%%%%%%" + config.get("TWISTER2_HTG_JOB"));
    JMDriverAgent.addWorkerListener(this);
    broadcast(messenger);
    LOG.info("Twister2 HTG Driver has finished execution.");
  }

  private void broadcast(IDriverMessenger messenger) {

    LOG.info("Testing broadcasting  ............................. ");
    try {
      LOG.info("Sleeping 5 seconds ....");
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    // construct an example protocol buffer message and broadcast it to all workers
    /*JobMasterAPI.NodeInfo nodeInfo =
        NodeInfoUtils.createNodeInfo("example.nodeIP", "rack-01", "dc-01");
      LOG.info("Broadcasting an example protocol buffer message: " + nodeInfo);
    messenger.broadcastToAllWorkers(nodeInfo);
        */

    HTGJobAPI.ExecuteMessage executeMessage = HTGJobAPI.ExecuteMessage.newBuilder()
        .setSubgraphName("sourcetaskgraph1")
        .build();

    LOG.info("Broadcasting an example protocol buffer message: " + executeMessage);

    messenger.broadcastToAllWorkers(executeMessage);
    try {
      LOG.info("Sleeping 5 seconds ....");
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    /*JobAPI.ComputeResource computeResource =
        ComputeResourceUtils.createComputeResource(10, 0.5, 2048, 2.0);

    LOG.info("Broadcasting another example protocol buffer message: " + computeResource);*/

    HTGJobAPI.ExecuteMessage executeMessage1 = HTGJobAPI.ExecuteMessage.newBuilder()
        .setSubgraphName("sinktaskgraph1")
        .build();

    LOG.info("Broadcasting another example protocol buffer message: " + executeMessage1);

    //messenger.broadcastToAllWorkers(computeResource);

    messenger.broadcastToAllWorkers(executeMessage1);
  }

  @Override
  public void workerMessageReceived(Any anyMessage, int senderID) {
    if (anyMessage.is(JobMasterAPI.NodeInfo.class)) {
      try {
        JobMasterAPI.NodeInfo nodeInfo = anyMessage.unpack(JobMasterAPI.NodeInfo.class);
        LOG.info("Received WorkerToDriver message from worker: " + senderID
            + ". NodeInfo: " + nodeInfo);

      } catch (com.google.protobuf.InvalidProtocolBufferException e) {
        LOG.log(Level.SEVERE, "Unable to unpack received protocol buffer message as broadcast", e);
      }
    } else if (anyMessage.is(JobAPI.ComputeResource.class)) {
      try {
        JobAPI.ComputeResource computeResource = anyMessage.unpack(JobAPI.ComputeResource.class);
        LOG.info("Received WorkerToDriver message from worker: " + senderID
            + ". ComputeResource: " + computeResource);

      } catch (InvalidProtocolBufferException e) {
        LOG.log(Level.SEVERE, "Unable to unpack received protocol buffer message as broadcast", e);
      }
    }
  }
}

