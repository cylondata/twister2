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

import java.util.List;
import java.util.concurrent.BlockingQueue;
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
import edu.iu.dsc.tws.proto.system.job.HTGJobAPI;

public class Twister2HTGDriver implements IDriver, WorkerListener {

  private static final Logger LOG = Logger.getLogger(Twister2HTGDriver.class.getName());

  private List<HTGJobAPI.ExecuteMessage> executeMessageList;
  private HTGJobAPI.HTGJob htgJob;

  //Newly Added for waiting for the response;
  private BlockingQueue<HTGJobAPI.ExecuteCompletedMessage> executeMessageQueue;
  private boolean executionCompleted = false;

  @Override
  public void execute(Config config, IScaler scaler, IDriverMessenger messenger) {

    Twister2HTGInstance twister2HTGInstance = Twister2HTGInstance.getTwister2HTGInstance();

    htgJob = twister2HTGInstance.getHtgJob();
    executeMessageList = twister2HTGInstance.getExecuteMessagesList();

    LOG.fine("HTG Job and Execution Order of the HTG Graph:"
        + twister2HTGInstance.getHtgJob() + "\t" + twister2HTGInstance.getExecuteMessagesList());

    JMDriverAgent.addWorkerListener(this);
    broadcast(messenger);
    LOG.info("Twister2 HTG Driver has finished execution.");
  }

  private void broadcast(IDriverMessenger messenger) {

    HTGJobAPI.ExecuteCompletedMessage msg;

    LOG.info("Testing HTG Driver  ............................. ");

    for (HTGJobAPI.ExecuteMessage executeMessage : executeMessageList) {

      LOG.info("Broadcasting execute message: " + executeMessage);

      try {
        LOG.info("Sleeping 5 seconds ....");
        Thread.sleep(5000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }

      messenger.broadcastToAllWorkers(executeMessage);
/*
      while (true) {
        try {
          msg = executeMessageQueue.take();
          break;
        } catch (InterruptedException e) {
          LOG.info("Unable to take the message from the queue");
        }
      }*/
    }

    try {
      LOG.info("Sleeping 5 seconds ....");
      Thread.sleep(5000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    HTGJobAPI.HTGJobCompletedMessage jobCompletedMessage = HTGJobAPI.HTGJobCompletedMessage
        .newBuilder().setHtgJobname("htg").build();
    LOG.info("Broadcasting job completed message: " + jobCompletedMessage);

    messenger.broadcastToAllWorkers(jobCompletedMessage);
  }

  @Override
  public void workerMessageReceived(Any anyMessage, int senderID) {

    if (anyMessage.is(HTGJobAPI.ExecuteCompletedMessage.class)) {
      try {
        HTGJobAPI.ExecuteCompletedMessage executeMessage = anyMessage.unpack(
            HTGJobAPI.ExecuteCompletedMessage.class);
        this.executeMessageQueue.put(executeMessage);
        this.executionCompleted = true;
      } catch (InvalidProtocolBufferException e) {
        LOG.log(Level.SEVERE, "Unable to unpack received protocol buffer message as broadcast", e);
      } catch (InterruptedException e) {
        LOG.log(Level.SEVERE, "Unable to insert message to the queue", e);
      }
    }
  }
}

