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
package edu.iu.dsc.tws.executor.core.checkpoint;

import java.nio.channels.SocketChannel;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import com.google.protobuf.Message;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.net.tcp.Progress;
import edu.iu.dsc.tws.common.net.tcp.StatusCode;
import edu.iu.dsc.tws.common.net.tcp.request.ConnectHandler;
import edu.iu.dsc.tws.common.net.tcp.request.MessageHandler;
import edu.iu.dsc.tws.common.net.tcp.request.RRClient;
import edu.iu.dsc.tws.common.net.tcp.request.RequestID;
import edu.iu.dsc.tws.proto.checkpoint.Checkpoint;

/**
 * This client will be shared by all the tasks running in the worker
 */
public final class CheckpointClient implements ConnectHandler {

  private static final Logger LOG = Logger.getLogger(CheckpointClient.class.getName());
  private static final String MSG_TASK_DISCOVERY = "MSG_TASK_DISCOVERY";

  private static volatile CheckpointClient instance;

  private RRClient taskClient;
  private Progress taskLooper;
  private Map<Integer, List<MessageHandler>> messageListeners;

  public static synchronized CheckpointClient getInstance(Config config, int workerId) {
    if (instance == null) {
      instance = new CheckpointClient(config, workerId);
    }
    return instance;
  }

  private CheckpointClient(Config config, int workerId) {
    taskLooper = new Progress();
    taskClient = new RRClient("localhost", 6789, config, taskLooper,
        workerId, this);
    taskClient.registerMessage(Checkpoint.CheckpointComplete.newBuilder());
    taskClient.registerResponseHandler(Checkpoint.TaskDiscovery.newBuilder(), new MessageHandler() {
      @Override
      public void onMessage(RequestID id, int workerId, Message message) {

      }
    });

    this.tryUntilConnected(this.taskClient, this.taskLooper, 5000);
    LOG.info("Connected to checkpoint agent...");
  }

  private boolean tryUntilConnected(RRClient client, Progress looper, long timeLimit) {
    long startTime = System.currentTimeMillis();
    long duration = 0;
    long sleepInterval = 50;

    // log interval in milliseconds
    long logInterval = 1000;
    long nextLogTime = logInterval;

    // allow the first connection attempt
    boolean connectionRefused = true;

    while (duration < timeLimit) {
      // try connecting
      if (connectionRefused) {
        client.tryConnecting();
        connectionRefused = false;
      }

      // loop to connect
      looper.loop();

      if (client.isConnected()) {
        return true;
      }

      try {
        Thread.sleep(sleepInterval);
      } catch (InterruptedException e) {
        LOG.warning("Sleep interrupted.");
      }

      if (client.isConnected()) {
        return true;
      }

      duration = System.currentTimeMillis() - startTime;

      if (duration > nextLogTime) {
        LOG.info("Still trying to connect to the Checkpoint Manager from Sink Task");
        nextLogTime += logInterval;
      }
    }
    return false;
  }

  @Override
  public void onError(SocketChannel channel) {
    LOG.severe("Failed to connect to checkpoint manager");
  }

  @Override
  public void onConnect(SocketChannel channel, StatusCode status) {
    LOG.info("Connected to checkpoint manager");
  }

  @Override
  public void onClose(SocketChannel channel) {
    LOG.info("Closed the connection to checkpoint manager");
  }
}
