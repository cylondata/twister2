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
package edu.iu.dsc.tws.master;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.collect.HashBiMap;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.net.tcp.ChannelHandler;
import edu.iu.dsc.tws.common.net.tcp.Progress;
import edu.iu.dsc.tws.common.net.tcp.Server;
import edu.iu.dsc.tws.common.net.tcp.StatusCode;
import edu.iu.dsc.tws.common.net.tcp.TCPMessage;
import edu.iu.dsc.tws.common.net.tcp.request.ByteUtils;
import edu.iu.dsc.tws.common.net.tcp.request.ConnectHandler;
import edu.iu.dsc.tws.common.net.tcp.request.MessageHandler;
import edu.iu.dsc.tws.common.net.tcp.request.RequestID;

/**
 * JMController class works in request/response messages
 * Workers and the client always send a request message, and
 * JobMaster sends a single response message to each request message
 * <p>
 * However, sometimes job master may send messages to workers that are not response messages
 * For example, a client request message may result in Job Master to send a message to all workers
 * <p>
 * Message Format:
 * RequestID (32 bytes), message type length, message type data, senderID (4 bytes), message data
 * message type is the class name of the protocol buffer for that message
 * <p>
 * RequestID is generated in request senders (worker or client),
 * and the same requestID is used in the response message.
 * <p>
 * When job master sends a message that is not a response to a request,
 * it uses the DUMMY_REQUEST_ID as the requestID.
 */

public class JMController {
  private static final Logger LOG = Logger.getLogger(JMController.class.getName());

  private Server server;

  /**
   * worker channels with workerIDs
   */
  private HashBiMap<SocketChannel, Integer> workerChannels = HashBiMap.create();

  /**
   * the job submitting client channel,
   */
  private SocketChannel clientChannel;

  /**
   * Keep track of the request handler using protocol buffer message types
   */
  private Map<String, MessageHandler> requestHandlers = new HashMap<>();

  /**
   * Message type name to builder
   */
  private Map<String, Message.Builder> messageBuilders = new HashMap<>();

  /**
   * requestID waiting map
   */
  private Map<RequestID, SocketChannel> requestChannels = new HashMap<>();

  /**
   * Job Master ID
   */
  public static final int JOB_MASTER_ID = -10;

  /**
   * The client id
   */
  public static final int CLIENT_ID = -100;

  /**
   * Connection handler
   */
  private ConnectHandler connectHandler;

  private Progress loop;

  private boolean jobMasterAssignsWorkerID;

  public JMController(Config cfg, String host, int port, Progress looper, ConnectHandler cHandler) {
    this.connectHandler = cHandler;
    this.loop = looper;
    server = new Server(cfg, host, port, loop, new Handler(), false);
    jobMasterAssignsWorkerID = JobMasterContext.jobMasterAssignsWorkerIDs(cfg);
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
   *
   * @param requestID request id
   * @param message message
   * @return true if response was accepted
   */
  public boolean sendResponse(RequestID requestID, Message message) {

    if (!requestChannels.containsKey(requestID)) {
      LOG.log(Level.SEVERE, "Trying to send a response to non-existing request");
      return false;
    }

    SocketChannel channel = requestChannels.get(requestID);

    if (channel == null) {
      LOG.log(Level.SEVERE, "Channel is NULL for response");
    }

    if (workerChannels.get(channel) != null) {
      LOG.log(Level.WARNING, "Failed to send response on disconnected socket");
      return false;
    }

    byte[] data = message.toByteArray();
    String messageType = message.getDescriptorForType().getFullName();

    // lets serialize the message
    int capacity = requestID.getId().length + data.length + 4 + messageType.getBytes().length + 4;
    ByteBuffer buffer = ByteBuffer.allocate(capacity);
    // we send message id, worker id and data
    buffer.put(requestID.getId());
    // pack the name of the message
    ByteUtils.packString(messageType, buffer);
    // pack the worker id
    buffer.putInt(JOB_MASTER_ID);
    // pack data
    buffer.put(data);

    TCPMessage request = server.send(channel, buffer, capacity, 0);

    if (request != null) {
      requestChannels.remove(requestID);
      return true;
    } else {
      return false;
    }
  }

  /**
   * Send message to a worker or to the client
   *
   * @param message message
   * @return true if response was accepted
   */
  public boolean sendMessage(Message message, int targetID) {

    SocketChannel channel;
    if (targetID == CLIENT_ID) {
      if (clientChannel == null) {
        LOG.severe("Trying to send a message to the client, but it has not connected yet.");
        return false;
      }
      channel = clientChannel;
    } else if (workerChannels.containsValue(targetID)) {
      channel = workerChannels.inverse().get(targetID);
    } else {
      LOG.severe("Trying to send a message to a worker that has not connected yet. workerID: "
          + targetID);
      return false;
    }

    // this is most likely not needed, but just to make sure
    if (channel == null) {
      LOG.log(Level.SEVERE, "Channel is NULL for response");
      return false;
    }

    byte[] data = message.toByteArray();
    String messageType = message.getDescriptorForType().getFullName();

    // since this is not a request/response message, we put the dummy request id
    RequestID dummyRequestID = RequestID.DUMMY_REQUEST_ID;

    // lets serialize the message
    int capacity = dummyRequestID.getId().length + data.length + messageType.getBytes().length + 8;
    ByteBuffer buffer = ByteBuffer.allocate(capacity);
    // we send message id, worker id and data
    buffer.put(dummyRequestID.getId());
    // pack the name of the message
    ByteUtils.packString(messageType, buffer);
    // pack the worker id
    buffer.putInt(JOB_MASTER_ID);
    // pack data
    buffer.put(data);

    TCPMessage tcpMessage = server.send(channel, buffer, capacity, 0);

    return tcpMessage == null ? false : true;
  }

  private class Handler implements ChannelHandler {
    @Override
    public void onError(SocketChannel channel) {
      workerChannels.remove(channel);
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
//      workerChannels.add(channel);
      connectHandler.onConnect(channel, status);
    }

    @Override
    public void onClose(SocketChannel channel) {
      workerChannels.remove(channel);
      connectHandler.onClose(channel);
    }

    @Override
    public void onReceiveComplete(SocketChannel channel, TCPMessage readRequest) {
      // read the request id and worker id
      // read the id and message
      ByteBuffer data = readRequest.getByteBuffer();

      // read requestID
      byte[] requestIDBytes = new byte[RequestID.ID_SIZE];
      data.get(requestIDBytes);
      RequestID requestID = RequestID.fromBytes(requestIDBytes);

      // unpack the string
      String messageType = ByteUtils.unPackString(data);

      // now get the worker id of the sender
      int senderID = data.getInt();

      // it can be a worker that will get its id from the job master
      senderID = addToChannelListAssignIdIfRequired(channel, senderID);

      Message.Builder builder = messageBuilders.get(messageType);
      if (builder == null) {
        throw new RuntimeException("Received response without a registered response");
      }

      try {
        builder.clear();

        // size of the header
        int headerLength = 8 + requestIDBytes.length + messageType.getBytes().length;
        int dataLength = readRequest.getLength() - headerLength;

        // get data
        byte[] messageDataBytes = new byte[dataLength];
        data.get(messageDataBytes);

        // build protocol buffer message
        builder.mergeFrom(messageDataBytes);
        Message m = builder.build();

        if (channel == null) {
          LOG.log(Level.SEVERE, "Chanel on receive is NULL");
        }
        LOG.log(Level.FINEST, String.format("Adding channel %s", new String(requestIDBytes)));
        requestChannels.put(requestID, channel);

        MessageHandler handler = requestHandlers.get(messageType);
        handler.onMessage(requestID, senderID, m);
      } catch (InvalidProtocolBufferException e) {
        LOG.log(Level.SEVERE, "Failed to build a message", e);
      }
    }

    @Override
    public void onSendComplete(SocketChannel channel, TCPMessage writeRequest) {

    }
  }

  /**
   * add to channel list if this is the first message from this worker
   * assign a worker ID if the job master assigns IDs
   */
  private int addToChannelListAssignIdIfRequired(SocketChannel channel, int senderID) {
    // if the channel already exist, do nothing
    // it means that the channel for this worker already added
    if (workerChannels.containsKey(channel)) {
      return senderID;
    }

    // if it is the submitting client
    // set it, no need to check whether it is already set
    // since it does not harm setting again
    if (senderID == CLIENT_ID) {
      clientChannel = channel;
      return senderID;
    }

    // assign a new worker ID to this new worker
    if (jobMasterAssignsWorkerID) {
      int newWorkerID = workerChannels.size();
      workerChannels.put(channel, newWorkerID);
      return newWorkerID;
    } else {
      workerChannels.put(channel, senderID);
      return senderID;
    }

  }
}
