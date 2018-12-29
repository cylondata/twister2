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

import com.google.common.collect.HashBiMap;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.net.tcp.ChannelHandler;
import edu.iu.dsc.tws.common.net.tcp.Progress;
import edu.iu.dsc.tws.common.net.tcp.Server;
import edu.iu.dsc.tws.common.net.tcp.StatusCode;
import edu.iu.dsc.tws.common.net.tcp.TCPMessage;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;

/**
 * RRServer class is used by Job Master
 * It works in request/response messaging
 *
 * Workers and the client always send a request message, and
 * JobMaster sends a single response message to each request message
 *
 * However, sometimes job master may send messages to workers that are not response messages
 * For example, a client request message may result in Job Master sending a message to all workers
 *
 * Message Format:
 * RequestID (32 bytes), message type length, message type data, senderID (4 bytes), message data
 * message type is the class name of the protocol buffer for that message
 *
 * RequestID is generated in request senders (worker or client),
 * and the same requestID is used in the response message.
 *
 * When job master sends a message that is not a response to a request,
 * it uses the DUMMY_REQUEST_ID as the requestID.
 */
public class RRServer {
  private static final Logger LOG = Logger.getLogger(RRServer.class.getName());

  private Server server;

  /**
   * worker channels with workerIDs
   */
  private HashBiMap<SocketChannel, Integer> workerChannels = HashBiMap.create();

  /**
   * the driver channel,
   */
  private SocketChannel driverChannel;

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
   * Job Master ID
   */
  private int serverID;

  /**
   * The driver id
   */
  public static final int DRIVER_ID = -100;

  /**
   * Connection handler
   */
  private ConnectHandler connectHandler;

  /**
   * when register message is received from a worker that will get its workerID from Job Master
   * we keep its channel on this variable
   * then WorkerMonitor calls setWorkerChannel method and set the id of that channel
   */
  private SocketChannel workerChannelToRegister;

  private Progress loop;

  public RRServer(Config cfg, String host, int port, Progress looper, int serverID,
                  ConnectHandler cHandler) {
    this.connectHandler = cHandler;
    this.loop = looper;
    this.serverID = serverID;
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

    if (!workerChannels.containsKey(channel) && !channel.equals(driverChannel)) {
      LOG.log(Level.WARNING, "Failed to send response on disconnected socket");
      return false;
    }

    TCPMessage tcpMessage = sendMessage(message, requestID, channel);

    if (tcpMessage != null) {
      requestChannels.remove(requestID);
      return true;
    } else {
      return false;
    }
  }

  /**
   * Send a non-response message to a worker or to the client
   * @param message message
   * @return true if response was accepted
   */
  public boolean sendMessage(Message message, int targetID) {

    SocketChannel channel;
    if (targetID == DRIVER_ID) {
      if (driverChannel == null) {
        LOG.severe("Trying to send a message to the driver, but it has not connected yet.");
        return false;
      }
      channel = driverChannel;
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

    // since this is not a request/response message, we put the dummy request id
    RequestID dummyRequestID = RequestID.DUMMY_REQUEST_ID;

    TCPMessage tcpMessage = sendMessage(message, dummyRequestID, channel);

    return tcpMessage == null ? false : true;
  }

  private TCPMessage sendMessage(Message message, RequestID requestID, SocketChannel channel) {
    byte[] data = message.toByteArray();
    String messageType = message.getDescriptorForType().getFullName();

    // lets serialize the message
    int capacity = requestID.getId().length + data.length + messageType.getBytes().length + 8;
    ByteBuffer buffer = ByteBuffer.allocate(capacity);
    // we send message id, worker id and data
    buffer.put(requestID.getId());
    // pack the name of the message
    ByteUtils.packString(messageType, buffer);
    // pack the worker id
    buffer.putInt(serverID);
    // pack data
    buffer.put(data);

    return server.send(channel, buffer, capacity, 0);
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
//      socketChannels.add(channel);
      connectHandler.onConnect(channel, status);
    }

    @Override
    public void onClose(SocketChannel channel) {
      workerChannels.remove(channel);
      connectHandler.onClose(channel);

      if (channel.equals(driverChannel)) {
        driverChannel = null;
      }
    }

    @Override
    public void onReceiveComplete(SocketChannel channel, TCPMessage readRequest) {
      if (channel == null) {
        LOG.log(Level.SEVERE, "Chanel on receive is NULL");
      }

      // read headers amd the message
      ByteBuffer data = readRequest.getByteBuffer();

      // read requestID
      byte[] requestIDBytes = new byte[RequestID.ID_SIZE];
      data.get(requestIDBytes);
      RequestID requestID = RequestID.fromBytes(requestIDBytes);

      // unpack the string
      String messageType = ByteUtils.unPackString(data);

      // now get sender worker id
      int senderID = data.getInt();

      Message.Builder builder = messageBuilders.get(messageType);
      if (builder == null) {
        throw new RuntimeException("Received response without a registered response");
      }

      try {
        builder.clear();

        // size of the header
        int headerLength = 8 + requestIDBytes.length + messageType.getBytes().length;
        int dataLength = readRequest.getLength() - headerLength;

        // reconstruct protocol buffer message
        byte[] d = new byte[dataLength];
        data.get(d);
        builder.mergeFrom(d);
        Message message = builder.build();

        // save this channel
        saveChannel(channel, senderID, message);

        LOG.log(Level.FINEST, String.format("Adding channel %s", new String(requestIDBytes)));
        requestChannels.put(requestID, channel);

        MessageHandler handler = requestHandlers.get(messageType);
        handler.onMessage(requestID, senderID, message);
      } catch (InvalidProtocolBufferException e) {
        LOG.log(Level.SEVERE, "Failed to build a message", e);
      }
    }

    @Override
    public void onSendComplete(SocketChannel channel, TCPMessage writeRequest) {

    }
  }

  /**
   * add the channel that newly got its register message and its workerID assigned
   * @param workerID
   */
  public void setWorkerChannel(int workerID) {

    // if there is another channel for this worker already, replace it with this one
    if (workerChannels.inverse().containsKey(workerID)) {
      LOG.warning(String.format("While there is a channel for workerID[%d], "
          + "another channel connected from the same worker. Replacing older one. ", workerID));
    }

    workerChannels.forcePut(workerChannelToRegister, workerID);
    workerChannelToRegister = null;
  }

  /**
   * remove the channel when the worker is removed
   * @param workerID
   */
  public void removeWorkerChannel(int workerID) {
    workerChannels.inverse().remove(workerID);
  }

  /**
   * save if it is a new channel
   * if it is a driver channel, save it in that variable
   * if it is a new worker channel that will get its id from the job master
   * save it in the temporary variable
   * otherwise add it to channel list
   */
  private void saveChannel(SocketChannel channel, int senderID, Message message) {

    // if the channel already exist, do nothing
    if (workerChannels.containsKey(channel)) {
      return;
    }

    // if it is the driver
    // set it, no need to check whether it is already set
    // since it does not harm setting again
    if (senderID == DRIVER_ID) {
      driverChannel = channel;
      LOG.info("Message received from submitting client. Channel set.");
      return;
    }

    // if this is RegisterWorker message and JobMaster assigns workerIDs
    // keep the channel in the variable
    if (senderID == RRClient.WORKER_UNASSIGNED_ID
        && message instanceof JobMasterAPI.RegisterWorker) {

      this.workerChannelToRegister = channel;
      return;
    }

    // if there is already a channel for this worker,
    // replace it with this one
    if (workerChannels.inverse().containsKey(senderID)) {
      LOG.warning(String.format("While there is a channel for workerID[%d], "
          + "another channel connected from the same worker. Replacing older one. ", senderID));
    }

    workerChannels.forcePut(channel, senderID);
  }
}
