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
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.exceptions.net.BlockingSendException;
import edu.iu.dsc.tws.api.exceptions.net.BlockingSendFailureReason;
import edu.iu.dsc.tws.api.net.StatusCode;
import edu.iu.dsc.tws.api.net.request.ConnectHandler;
import edu.iu.dsc.tws.api.net.request.MessageHandler;
import edu.iu.dsc.tws.api.net.request.RequestID;
import edu.iu.dsc.tws.common.net.tcp.ChannelHandler;
import edu.iu.dsc.tws.common.net.tcp.Client;
import edu.iu.dsc.tws.common.net.tcp.Progress;
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
   * The client id
   */
  private int workerID;

  /**
   * Connection handler
   */
  private ConnectHandler connectHandler;

  private final Object responseWaitObject = new Object();
  private RequestID requestIdOfWaitedResponse = null;
  private Message waitedResponseMessage = null;

  /**
   * a flag to show whether the response received for the message that waits for the response
   */
  private boolean responseReceived = false;

  /**
   * a flag to show whether an error occurred when sending sending a message that needs a response
   */
  private boolean errorWhenSending = false;

  /**
   * The communicationProgress loop
   */
  private Progress loop;

  public RRClient(String host, int port, Config cfg, Progress looper,
                  int wId, ConnectHandler cHandler) {
    this.connectHandler = cHandler;
    this.workerID = wId;
    this.loop = looper;
    client = new Client(host, port, cfg, looper, new Handler(), false);
  }

  public void setHostAndPort(String host, int port) {
    client.setHostAndPort(host, port);
  }

  public boolean connect() {
    return client.connect();
  }

  public void tryConnecting() {
    client.tryConnecting();
  }

  public void disconnect() {
    client.disconnect();
  }

  public void disconnectGraceFully(long waitTime) {
    client.disconnectGraceFully(waitTime);
  }

  public boolean isConnected() {
    return client.isConnected();
  }

  /**
   * return requestID and response message
   * throw an exception with the failure reason
   * @param message message
   * @param waitLimit waitlimit
   * @return request id
   */
  public Tuple<RequestID, Message> sendRequestWaitResponse(Message message, long waitLimit)
      throws BlockingSendException {

    // if this method is already called and waiting for a response
    if (requestIdOfWaitedResponse != null) {
      throw new BlockingSendException(BlockingSendFailureReason.ALREADY_SENDING_ANOTHER_MESSAGE,
          "Already sending another message.", null);
    }

    synchronized (responseWaitObject) {
      responseReceived = false;
      errorWhenSending = false;

      RequestID requestID = sendRequest(message);
      requestIdOfWaitedResponse = requestID;

      if (requestIdOfWaitedResponse == null) {
        throw new BlockingSendException(BlockingSendFailureReason.ERROR_WHEN_TRYING_TO_SEND,
            "Problem when trying to send the message.", null);
      }

      try {
        responseWaitObject.wait(waitLimit);
        if (errorWhenSending) {
          throw new BlockingSendException(BlockingSendFailureReason.CONNECTION_LOST_WHEN_SENDING,
              "Connection lost when sending the message.", null);
        }
        if (!responseReceived) {
          throw new BlockingSendException(BlockingSendFailureReason.TIME_LIMIT_REACHED,
              "Response not received for Wait limit[" + waitLimit + "ms].", null);
        }
      } catch (InterruptedException e) {
        throw new BlockingSendException(BlockingSendFailureReason.EXCEPTION_WHEN_WAITING,
            "Exception when waiting the response.", e);
      }

      return new Tuple(requestID, waitedResponseMessage);
    }
  }

  public RequestID sendRequest(Message message) {
    if (!client.isConnected()) {
      return null;
    }
    String messageType = message.getDescriptorForType().getFullName();
    if (!messageBuilders.containsKey(messageType)) {
      throw new RuntimeException("Message without a message builder");
    }

    RequestID id = RequestID.generate();
    byte[] data = message.toByteArray();

    // lets serialize the message
    int capacity = id.getId().length + data.length + 4 + messageType.getBytes().length + 4;
    ByteBuffer buffer = ByteBuffer.allocate(capacity);
    // we send message id, worker id and data
    buffer.put(id.getId());
    // pack the name of the message
    ByteUtils.packString(messageType, buffer);
    // pack the worker id
    buffer.putInt(workerID);
    // pack data
    buffer.put(data);

    TCPMessage request = client.send(channel, buffer, capacity, 0);
    if (request != null) {
      loop.wakeup();
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

  /**
   * Register a message type for sending
   * @param builder the message type
   */
  public void registerMessage(Message.Builder builder) {
    messageBuilders.put(builder.getDescriptorForType().getFullName(), builder);
  }

  private class Handler implements ChannelHandler {
    @Override
    public void onError(SocketChannel ch, StatusCode status) {
      LOG.severe("Error happened");
      loop.removeAllInterest(ch);

      try {
        ch.close();
        LOG.log(Level.FINEST, "Closed the channel: " + ch);
      } catch (IOException e) {
        LOG.log(Level.SEVERE, "Failed to close the channel: " + ch, e);
      }
      connectHandler.onError(ch, status);
      // notify if this response is waited
      synchronized (responseWaitObject) {
        requestIdOfWaitedResponse = null;
        errorWhenSending = true;
        waitedResponseMessage = null;
        responseWaitObject.notify();
      }
    }

    @Override
    public void onConnect(SocketChannel ch) {
      channel = ch;
      connectHandler.onConnect(ch);
    }

    @Override
    public void onClose(SocketChannel ch) {
      connectHandler.onClose(ch);
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
        throw new RuntimeException("Message builder should be registered, "
            + "see registerMessage method");
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

        MessageHandler handler = responseHandlers.get(messageType);
        if (handler == null) {
          LOG.log(Level.WARNING, "Failed to get handler for message: " + messageType);
        } else {
          handler.onMessage(requestID, serverWorkerId, m);
        }

        // notify if this response is waited
        synchronized (responseWaitObject) {
          if (requestID.equals(requestIdOfWaitedResponse)) {
            requestIdOfWaitedResponse = null;
            responseReceived = true;
            waitedResponseMessage = m;
            responseWaitObject.notify();
          }
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
