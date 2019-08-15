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
package edu.iu.dsc.tws.master.server;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.net.StatusCode;
import edu.iu.dsc.tws.api.net.request.ConnectHandler;
import edu.iu.dsc.tws.api.net.request.MessageHandler;
import edu.iu.dsc.tws.api.net.request.RequestID;
import edu.iu.dsc.tws.common.net.tcp.ChannelHandler;
import edu.iu.dsc.tws.common.net.tcp.Progress;
import edu.iu.dsc.tws.common.net.tcp.TCPMessage;
import edu.iu.dsc.tws.common.net.tcp.request.ByteUtils;
import edu.iu.dsc.tws.common.net.tcp.request.RRClient;
import edu.iu.dsc.tws.common.net.tcp.request.RRServer;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;

/**
 * JMRRServer class is used by Job Master
 * This class is extended from RRServer class.
 * It works in request/response messaging
 *
 * Workers and the client always send a request message, and
 * JobMaster sends a single response message to each request message
 *
 * However, sometimes job master may send messages to workers that are not response messages
 * For example, the driver in Job Master may send messages to workers
 * The messages that are initiated from Job Master are one-way messages
 * They don't have response messages
 *
 * Message Format:
 * RequestID (32 bytes), message type length, message type data, senderID (4 bytes), message data
 * message type is the class name of the protocol buffer for that message
 *
 * RequestID is generated in request senders (workers or the client),
 * and the same requestID is used in the response message.
 *
 * When job master sends a message that is not a response to a request,
 * it uses the DUMMY_REQUEST_ID as the requestID.
 */
public class JMRRServer extends RRServer {

  private static final Logger LOG = Logger.getLogger(JMRRServer.class.getName());

  public JMRRServer(Config cfg, String host, int port, Progress looper, int serverID,
                  ConnectHandler cHandler) {
    super(cfg, host, port, looper, serverID, cHandler);
  }


  private class Handler implements ChannelHandler {
    @Override
    public void onError(SocketChannel channel) {
      workerChannels.remove(channel);
      connectedChannels.remove(channel);
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
      connectedChannels.add(channel);
      connectHandler.onConnect(channel, status);
    }

    @Override
    public void onClose(SocketChannel channel) {
      workerChannels.remove(channel);
      connectedChannels.remove(channel);
      connectHandler.onClose(channel);

      if (channel.equals(clientChannel)) {
        clientChannel = null;
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
      pendingSendCount--;
    }
  }
  /**
   * save if it is a new channel
   * if it is a client channel, save it in that variable
   * if it is a new worker channel that will get its id from the job master
   * save it in the temporary variable
   * otherwise add it to the channel list
   */
  public void saveChannel(SocketChannel channel, int senderID, Message message) {

    // if a worker is coming from failure and re-registering,
    // replace the previous channel with this one
    if (message instanceof JobMasterAPI.RegisterWorker
        && ((JobMasterAPI.RegisterWorker) message).getFromFailure()) {

      // first remove the previous channel if any
      removeWorkerChannel(senderID);

      LOG.fine("Worker is re-registering after failure, previous channel is reset.");
      workerChannels.forcePut(channel, senderID);
      return;
    }

    // if the channel already exist, do nothing
    if (workerChannels.containsKey(channel)) {
      return;
    }

    // if it is the client
    // set it, no need to check whether it is already set
    // since it does not harm setting again
    if (senderID == CLIENT_ID) {
      clientChannel = channel;
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
