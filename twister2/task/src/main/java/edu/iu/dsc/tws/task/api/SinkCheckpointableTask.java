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
import java.util.ArrayList;
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
import edu.iu.dsc.tws.task.streaming.BaseStreamSink;

public abstract class SinkCheckpointableTask extends BaseStreamSink implements ICheckPointable {
  private static final long serialVersionUID = -254264903890214728L;

  private static final Logger LOG = Logger.getLogger(SinkCheckpointableTask.class.getName());

  public TaskContext ctx;
  public Config config;

  private RRClient taskClient;
  private Progress taskLooper;

  public Snapshot snapshot;

  /**
   * to control the connection error when we repeatedly try connecting
   */
  private boolean connectionRefused = false;

  public void connect(Config cfg, TaskContext context) {
    this.ctx = context;

    taskLooper = new Progress();

    taskClient = new RRClient("localhost", 6789, cfg, taskLooper,
        context.taskId(), new ClientConnectHandler());

    taskClient.registerMessage(Checkpoint.CheckpointComplete.newBuilder());
    taskClient.registerResponseHandler(Checkpoint.TaskDiscovery.newBuilder(),
        new ClientMessageHandler());

    tryUntilConnected(taskClient, taskLooper, 5000);
    sendTaskDiscoveryMessage();
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
        LOG.info("Still trying to connect to the Checkpoint Manager from Sink Task");
        nextLogTime += logInterval;
      }
    }
    return false;
  }

  public class ClientConnectHandler implements ConnectHandler {
    @Override
    public void onError(SocketChannel channel) {
      LOG.severe("TaskClientConnectHandler error thrown inside Sink Task");
    }

    @Override
    public void onConnect(SocketChannel channel, StatusCode status) {
      LOG.info("TaskClientConnectHandler inside Sink Task got connected");

    }

    @Override
    public void onClose(SocketChannel channel) {
      LOG.info("ClientConnect Handler inside Sink task closed");
    }
  }

  public class ClientMessageHandler implements MessageHandler {
    @Override
    public void onMessage(RequestID id, int workerId, Message message) {
      LOG.info("TaskClientMessageHandler inside sink task got message from worker ID "
          + workerId);

    }
  }

  private void sendTaskDiscoveryMessage() {
    Checkpoint.TaskDiscovery message = Checkpoint.TaskDiscovery.newBuilder()
        .setTaskID(ctx.taskId())
        .setTaskType(Checkpoint.TaskDiscovery.TaskType.SINK)
        .setParrallelism(this.ctx.getParallelism())
        .build();

    taskClient.sendRequest(message);
    taskLooper.loop();
  }

  /**
   * This method should get called when a valid checkpoint is made.
   */
  public void receivedValidBarrier(IMessage message) {

    Object messageContent = message.getContent();

    if (messageContent instanceof ArrayList) {

      @SuppressWarnings("unchecked")
      ArrayList<Integer> messageArray = (ArrayList<Integer>) messageContent;

      Checkpoint.CheckpointComplete checkpointCompleteMessage
          = Checkpoint.CheckpointComplete.newBuilder()
          .setCheckpointComplete(true)
          .setCurrentBarrierID(messageArray.get(0))
          .setSinkID(ctx.taskId())
          .build();

      taskClient.sendRequest(checkpointCompleteMessage);
      taskLooper.loop();
    }
  }

  public void addState(String key, Object value) {
    if (snapshot == null) {
      snapshot = new Snapshot();
    }
    snapshot.addState(key, value);
  }

  public Object getState(String key) {
    return snapshot.getState(key);
  }

  @Override
  public Snapshot getSnapshot() {
    return snapshot;
  }

  @Override
  public void restoreSnapshot(Snapshot newsnapshot) {
    this.snapshot = newsnapshot;
  }

  public abstract void addCheckpointableStates();

}
