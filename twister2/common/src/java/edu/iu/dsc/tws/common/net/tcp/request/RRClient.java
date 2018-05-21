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

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.net.tcp.ChannelHandler;
import edu.iu.dsc.tws.common.net.tcp.Client;
import edu.iu.dsc.tws.common.net.tcp.Progress;
import edu.iu.dsc.tws.common.net.tcp.StatusCode;
import edu.iu.dsc.tws.common.net.tcp.TCPMessage;

/**
 * RequestID response client
 */
public class RRClient {
  private static final Logger LOG = Logger.getLogger(RRClient.class.getName());

  /**
   * The underlying client
   */
  private Client client;

  /**
   * The socket channler of the client
   */
  private SocketChannel channel;

  /**
   * Keep track of the response handler using protocol buffer message types
   */
  private Map<String, MessageHandler> responseHandlers = new HashMap<>();

  /**
   * Message builders
   */
  private Map<String, Message.Builder> messageBuilders = new HashMap<>();

  /**
   * Weather we are connected
   */
  private boolean connected = false;

  /**
   * The client id
   */
  private int workerId;

  public RRClient(String host, int port, Config cfg, Progress looper, int wId) {
    this.workerId = wId;
    client = new Client(host, port, cfg, looper, new Handler(), false);
  }

  public RequestID sendRequest(Message message) {
    if (!connected) {
      return null;
    }
    String messageType = message.getDescriptorForType().getFullName();
    if (!responseHandlers.containsKey(messageType)) {
      throw new RuntimeException("Message without a response handler");
    }

    RequestID id = RequestID.generate();
    byte[] data = message.toByteArray();

    // lets serialize the message
    int capacity = id.getId().length + data.length + 4;
    ByteBuffer buffer = ByteBuffer.allocate(capacity);
    // we send message id, worker id and data
    buffer.put(id.getId());
    // pack the name of the message
    ByteUtils.packString(messageType, buffer);
    // pack the worker id
    buffer.putInt(workerId);
    // pack data
    buffer.put(data);

    TCPMessage request = client.send(channel, buffer, capacity, 0);
    if (request != null) {
      return id;
    } else {
      return null;
    }
  }

  /**
   * Register a response handler to a specific message type
   * @param builder the response message type
   * @param handler the message callback
   */
  public void registerResponseHandler(Message.Builder builder, MessageHandler handler) {
    responseHandlers.put(builder.getDescriptorForType().getFullName(), handler);
    messageBuilders.put(builder.getDescriptorForType().getFullName(), builder);
  }

  private class Handler implements ChannelHandler {
    @Override
    public void onError(SocketChannel ch) {
      connected = false;
      LOG.severe("Error happened");
    }

    @Override
    public void onConnect(SocketChannel ch, StatusCode status) {
      channel = ch;
      connected = true;
    }

    @Override
    public void onClose(SocketChannel ch) {
      connected = false;
    }

    @Override
    public void onReceiveComplete(SocketChannel ch, TCPMessage readRequest) {
      // read the id and message
      ByteBuffer data = readRequest.getByteBuffer();
      byte[] id = new byte[RequestID.ID_SIZE];
      data.get(id);

      // now read the mesage type
      String messageType = ByteUtils.unPackString(data);

      // now get the worker id
      int serverWorkerId = data.getInt();

      RequestID requestID = RequestID.fromBytes(id);
      Message.Builder builder = messageBuilders.get(messageType);

      if (builder == null) {
        throw new RuntimeException("Received response without a registered response");
      }

      try {
        builder.mergeFrom(data.array());
        Message m = builder.build();

        MessageHandler handler = responseHandlers.get(messageType);

        if (handler == null) {
          LOG.log(Level.WARNING, "Failed to get handler for message: " + messageType);
        } else {
          handler.onMessage(requestID, serverWorkerId, m);
        }
      } catch (InvalidProtocolBufferException e) {
        LOG.log(Level.SEVERE, "Failed to build a message", e);
      }
    }

    @Override
    public void onSendComplete(SocketChannel ch, TCPMessage writeRequest) {
      // we do nothing
    }
  }
}
