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
package edu.iu.dsc.tws.common.net.tcp.client;

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
import edu.iu.dsc.tws.common.net.tcp.RequestID;
import edu.iu.dsc.tws.common.net.tcp.ResponseHandler;
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
   * Keep track of the request
   */
  private Map<RequestID, ResponseHandler> requests = new HashMap<>();

  /**
   * Keep track of the response handler using protocol buffer message types
   */
  private Map<Message.Builder, ResponseHandler> staticResponseHandlers = new HashMap<>();

  /**
   * Message builders
   */
  private Map<RequestID, Message.Builder> messageBuilders = new HashMap<>();

  private int edgeId;

  private boolean connected = false;

  /**
   * The client id
   */
  private int clientId;

  public RRClient(String host, int port, Config cfg, Progress looper) {
    client = new Client(host, port, cfg, looper, new Handler());
  }

  public RequestID sendRequest(Message message) {
    if (!connected) {
      return null;
    }

    Message.Builder key = message.newBuilderForType();
    if (!staticResponseHandlers.containsKey(key)) {
      throw new RuntimeException("Message without a response handler");
    }

    RequestID id = RequestID.generate();
    byte[] data = message.toByteArray();

    // lets serialize the message
    int capacity = id.getId().length + data.length;
    ByteBuffer buffer = ByteBuffer.allocate(capacity);
    buffer.put(id.getId());
    buffer.put(data);

    TCPMessage request = client.send(channel, buffer, capacity, edgeId);
    if (request != null) {
      ResponseHandler handler = staticResponseHandlers.get(key);
      messageBuilders.put(id, key);
      requests.put(id, handler);
      return id;
    } else {
      return null;
    }
  }

  public RequestID sendRequest(Message message, ResponseHandler handler) {
    if (!connected) {
      return null;
    }

    RequestID id = RequestID.generate();

    byte[] data = message.toByteArray();

    // lets serialize the message
    int capacity = id.getId().length + data.length;
    ByteBuffer buffer = ByteBuffer.allocate(capacity);
    buffer.put(id.getId());
    buffer.put(data);

    TCPMessage request = client.send(channel, buffer, capacity, edgeId);
    if (request != null) {
      requests.put(id, handler);
      messageBuilders.put(id, message.newBuilderForType());
      return id;
    } else {
      return null;
    }
  }

  public void registerRequestHandler(Message.Builder builder, ResponseHandler handler) {
    staticResponseHandlers.put(builder, handler);
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

      RequestID requestID = RequestID.fromBytes(id);
      Message.Builder builder = messageBuilders.get(requestID);

      if (builder == null) {
        throw new RuntimeException("Received response without a registered response");
      }

      try {
        builder.mergeFrom(data.array());
        Message m = builder.build();

        ResponseHandler handler = requests.get(requestID);

        handler.onMessage(m);

        // remove the references
        messageBuilders.remove(requestID);
        requests.remove(requestID);
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
