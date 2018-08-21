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
package edu.iu.dsc.tws.common.net.tcp.request;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.net.tcp.ChannelHandler;
import edu.iu.dsc.tws.common.net.tcp.Progress;
import edu.iu.dsc.tws.common.net.tcp.Server;
import edu.iu.dsc.tws.common.net.tcp.StatusCode;
import edu.iu.dsc.tws.common.net.tcp.TCPMessage;

/**
 * Request response server.
 */
public class RRServer {
  private static final Logger LOG = Logger.getLogger(RRServer.class.getName());

  private Server server;

  /**
   * Client id to channel
   */
  private List<SocketChannel> socketChannels = new ArrayList<>();

  /**
   * Keep track of the request handler using protocol buffer message types
   */
  private Map<String, MessageHandler> requestHandlers = new HashMap<>();

  /**
   * Message type name to builder
   */
  private Map<String, Message.Builder> messageBuilders = new HashMap<>();

  /**
   * Keep track of the requests
   */
  private Map<RequestID, SocketChannel> requestChannels = new HashMap<>();

  /**
   * Keep track of request id to worker
   */
  private Map<RequestID, Integer> requestToWorkers = new HashMap<>();

  /**
   * The worker id
   */
  private int workerId;

  /**
   * Connection handler
   */
  private ConnectHandler connectHandler;

  private Progress loop;

  public RRServer(Config cfg, String host, int port, Progress looper,
                  int wId, ConnectHandler cHandler) {
    this.connectHandler = cHandler;
    this.workerId = wId;
    this.loop = looper;
    server = new Server(cfg, host, port, loop, new Handler(), false);
  }

  public void registerRequestHandler(Message.Builder builder, MessageHandler handler) {
    requestHandlers.put(builder.getDescriptorForType().getFullName(), handler);
    messageBuilders.put(builder.getDescriptorForType().getFullName(), builder);
  }

  public void start() {
    server.start();
  }

  public void stop() {
    server.stop();
  }

  public void stopGraceFully(long waitTime) {
    server.stopGraceFully(waitTime);
  }

  /**
   * Send a response to a request id
   * @param id request id
   * @param message message
   * @return true if response was accepted
   */
  public boolean sendResponse(RequestID id, Message message) {
    LOG.log(Level.FINEST, String.format("Using channel %s", new String(id.getId())));

    if (!requestChannels.containsKey(id)) {
      LOG.log(Level.SEVERE, "Trying to send response to non-existing request");
      return false;
    }

    SocketChannel channel = requestChannels.get(id);

    if (channel == null) {
      LOG.log(Level.SEVERE, "Channel is NULL for response");
    }

    if (!socketChannels.contains(channel)) {
      LOG.log(Level.WARNING, "Failed to send response on disconnected socket");
      return false;
    }

    byte[] data = message.toByteArray();
    String messageType = message.getDescriptorForType().getFullName();

    // lets serialize the message
    int capacity = id.getId().length + data.length + 4 + messageType.getBytes().length + 4;
    ByteBuffer buffer = ByteBuffer.allocate(capacity);
    // we send message id, worker id and data
    buffer.put(id.getId());
    // pack the name of the message
    ByteUtils.packString(messageType, buffer);
    // pack the worker id
    buffer.putInt(workerId);
    // pack data
    buffer.put(data);

    TCPMessage request = server.send(channel, buffer, capacity, 0);

    if (request != null) {
      requestChannels.remove(id);
      return true;
    } else {
      return false;
    }
  }

  private class Handler implements ChannelHandler {
    @Override
    public void onError(SocketChannel channel) {
      socketChannels.remove(channel);
      connectHandler.onError(channel);

      loop.removeAllInterest(channel);

      try {
        channel.close();
        LOG.log(Level.FINEST, "Closed the channel: " + channel);
      } catch (IOException e) {
        LOG.log(Level.SEVERE, "Channel closed error: " + channel, e);
      }
    }

    @Override
    public void onConnect(SocketChannel channel, StatusCode status) {
      socketChannels.add(channel);
      connectHandler.onConnect(channel, status);
    }

    @Override
    public void onClose(SocketChannel channel) {
      socketChannels.remove(channel);
      connectHandler.onClose(channel);
    }

    @Override
    public void onReceiveComplete(SocketChannel channel, TCPMessage readRequest) {
      // read the request id and worker id
      // read the id and message
      ByteBuffer data = readRequest.getByteBuffer();
      byte[] id = new byte[RequestID.ID_SIZE];
      data.get(id);

      // unpack the string
      String messageType = ByteUtils.unPackString(data);

      // now get the worker id
      int clientId = data.getInt();

      RequestID requestID = RequestID.fromBytes(id);
      Message.Builder builder = messageBuilders.get(messageType);

      if (builder == null) {
        throw new RuntimeException("Received response without a registered response");
      }

      try {
        builder.clear();

        // size of the header
        int headerLength = 8 + id.length + messageType.getBytes().length;
        int dataLength = readRequest.getLength() - headerLength;

        byte[] d = new byte[dataLength];
        data.get(d);

        builder.mergeFrom(d);
        Message m = builder.build();

        if (channel == null) {
          LOG.log(Level.SEVERE, "Chanel on receive is NULL");
        }
        LOG.log(Level.FINEST, String.format("Adding channel %s", new String(id)));
        requestChannels.put(requestID, channel);

        MessageHandler handler = requestHandlers.get(messageType);
        handler.onMessage(requestID, clientId, m);
      } catch (InvalidProtocolBufferException e) {
        LOG.log(Level.SEVERE, "Failed to build a message", e);
      }
    }

    @Override
    public void onSendComplete(SocketChannel channel, TCPMessage writeRequest) {

    }
  }
}
