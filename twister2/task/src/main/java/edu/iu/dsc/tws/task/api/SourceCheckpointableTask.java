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
package edu.iu.dsc.tws.task.api;

import java.nio.channels.SocketChannel;
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
import edu.iu.dsc.tws.task.streaming.BaseStreamSource;

public abstract class SourceCheckpointableTask extends BaseStreamSource {
  private static final long serialVersionUID = -254264903510214728L;

  private static final Logger LOG = Logger.getLogger(SourceCheckpointableTask.class.getName());

  public TaskContext ctx;
  public Config config;

  public RRClient taskClient;
  public Progress taskLooper;

  private int currentBarrierID = 1;

  /**
   * to control the connection error when we repeatedly try connecting
   */
  private boolean connectionRefused = false;

  public void connect(Config cfg, TaskContext context) {
    this.ctx = context;
    this.config = cfg;

    taskLooper = new Progress();

    taskClient = new RRClient("localhost", 6789, cfg, taskLooper,
        context.taskId(), new TaskClientConnectHandler());

    taskClient.registerResponseHandler(Checkpoint.TaskDiscovery.newBuilder(),
        new TaskClientMessageHandler());

    taskClient.registerResponseHandler(Checkpoint.BarrierSend.newBuilder(),
        new BarrierClientMessageHandler());

    taskClient.registerResponseHandler(Checkpoint.BarrierSync.newBuilder(),
        new BarrierClientMessageHandler());

    tryUntilConnected(taskClient, taskLooper, 5000);
    sendTaskDiscoveryMessage();
  }

  public void checkForBarrier() {
    sendBarrierSyncMessage();
  }

  private boolean tryUntilConnected(RRClient client, Progress looper, long timeLimit) {
    long startTime = System.currentTimeMillis();
    long duration = 0;
    long sleepInterval = 50;

    // log interval in milliseconds
    long logInterval = 1000;
    long nextLogTime = logInterval;

    // allow the first connection attempt
    connectionRefused = true;

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
        LOG.info("Still trying to connect to the Checkpoint Manager from Source Task");
        nextLogTime += logInterval;
      }
    }
    return false;
  }

  public class TaskClientConnectHandler implements ConnectHandler {
    @Override
    public void onError(SocketChannel channel) {
      LOG.severe("TaskClientConnectHandler error thrown inside Source Task");
    }

    @Override
    public void onConnect(SocketChannel channel, StatusCode status) {
      LOG.info("TaskClientConnectHandler inside Source Task got connected");

    }

    @Override
    public void onClose(SocketChannel channel) {
      LOG.info("TaskClientConnect Handler inside Source task closed");
    }
  }

  public class TaskClientMessageHandler implements MessageHandler {
    @Override
    public void onMessage(RequestID id, int workerId, Message message) {
      LOG.info("TaskClientMessageHandler inside source task got message from worker ID "
          + workerId);

    }
  }

  public class BarrierClientMessageHandler implements MessageHandler {
    @Override
    public void onMessage(RequestID id, int workerId, Message message) {

      if (message instanceof Checkpoint.BarrierSend) {
        Checkpoint.BarrierSend barrierSend = (Checkpoint.BarrierSend) message;

        if (barrierSend.getSendBarrier()) {
          currentBarrierID = barrierSend.getCurrentBarrierID();
//          LOG.info("Signal to emit barrier with ID : " + currentBarrierID + " received");

          emitBarrier();

          currentBarrierID++;
        }
      }
    }
  }

  private void sendTaskDiscoveryMessage() {
    Checkpoint.TaskDiscovery message = Checkpoint.TaskDiscovery.newBuilder()
        .setTaskID(ctx.taskId())
        .setTaskType(Checkpoint.TaskDiscovery.TaskType.SOURCE)
        .setParrallelism(ctx.getParallelism())
        .build();

    taskClient.sendRequest(message);
    taskLooper.loop();
  }

  private void sendBarrierSyncMessage() {
    Checkpoint.BarrierSync message = Checkpoint.BarrierSync.newBuilder()
        .setCurrentBarrierID(currentBarrierID)
        .setTaskID(ctx.taskId())
        .build();

    taskClient.sendRequest(message);
    taskLooper.loop();
  }

  /**
   * This will have the method to emit barrier in to the outgoing channel of the source task
   */
  private void emitBarrier() {
    LOG.info("Sending barrier from source task ID : " + ctx.taskId());

    Checkpoint.CheckpointBarrier checkpointBarrierMessage
        = Checkpoint.CheckpointBarrier.newBuilder()
        .setCurrentBarrierID(currentBarrierID)
        .build();

    ctx.writeBarrier("keyed-edge", checkpointBarrierMessage);

  }
}
