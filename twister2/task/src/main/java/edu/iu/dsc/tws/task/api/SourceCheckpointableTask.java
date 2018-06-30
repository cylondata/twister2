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

public abstract class SourceCheckpointableTask extends SourceTask {
  private static final long serialVersionUID = -254264903510214728L;

  private static final Logger LOG = Logger.getLogger(SourceCheckpointableTask.class.getName());

  public TaskContext ctx;
  public Config config;

  public RRClient client;
  public Progress looper;

  @Override
  public void prepare(Config cfg, TaskContext context) {
    this.ctx = context;

    client = new RRClient("localhost", 6789, cfg, looper,
        context.taskId(), new ClientConnectHandler());

    client.registerResponseHandler(Checkpoint.TaskDiscovery.newBuilder(),
        new ClientMessageHandler());

  }

  public class ClientConnectHandler implements ConnectHandler {
    @Override
    public void onError(SocketChannel channel) {
      LOG.severe("ClientConnectHandler error thrown inside Source Task");
    }

    @Override
    public void onConnect(SocketChannel channel, StatusCode status) {
      LOG.info("ClientConnectHandler inside Source Task got connected");

      Checkpoint.TaskDiscovery message = Checkpoint.TaskDiscovery.newBuilder()
          .setTaskID(ctx.taskId())
          .setTaskType(Checkpoint.TaskDiscovery.TaskType.SOURCE)
          .build();

      client.sendRequest(message);
    }

    @Override
    public void onClose(SocketChannel channel) {
      LOG.info("ClientConnect Handler inside Source task closed");
    }
  }

  public class ClientMessageHandler implements MessageHandler {
    @Override
    public void onMessage(RequestID id, int workerId, Message message) {
      LOG.info("ClientMessageHandler inside source task got message from worker ID "
          + workerId);

      client.disconnect();
    }
  }
}
