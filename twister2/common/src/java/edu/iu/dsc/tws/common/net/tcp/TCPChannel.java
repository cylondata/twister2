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
package edu.iu.dsc.tws.common.net.tcp;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.nio.channels.UnresolvedAddressException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.collect.HashBiMap;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;
import edu.iu.dsc.tws.api.net.StatusCode;
import edu.iu.dsc.tws.common.net.NetworkInfo;

/**
 * A channel implementation, that supports iSend and iRecv semantics.
 */
public class TCPChannel {
  private static final Logger LOG = Logger.getLogger(TCPChannel.class.getName());

  /**
   * Server
   */
  private Server server;

  /**
   * Client map
   */
  private Map<Integer, Client> clients;

  /**
   * The communicationProgress loop
   */
  private Progress looper;

  /**
   * The configuration
   */
  private Config config;

  /**
   * Network information
   */
  private NetworkInfo thisInfo;

  /**
   * Network info of all workers with worker id
   */
  private Map<Integer, NetworkInfo> networkInfoMap;

  /**
   * client channels with workerIDs
   */
  protected HashBiMap<Integer, SocketChannel> clientChannels = HashBiMap.create();

  /**
   * server channels with workerIDs
   */
  protected HashBiMap<Integer, SocketChannel> serverChannels = HashBiMap.create();

  // we use a pre-allocated set of buffers to send the hello messages to
  // the servers connected to by the client, so each client will send this message
  private List<ByteBuffer> helloSendByteBuffers;
  private List<ByteBuffer> helloReceiveByteBuffers;

  private int clientsCompleted = 0;
  private int clientsConnected = 0;

  /**
   * Start the channel with the given network information
   * @param cfg configuration
   * @param info network information
   */
  public TCPChannel(Config cfg, NetworkInfo info) {
    config = cfg;
    thisInfo = info;

    clients = new HashMap<>();
    looper = new Progress();

    networkInfoMap = new HashMap<>();
    helloSendByteBuffers = new ArrayList<>();
    helloReceiveByteBuffers = new ArrayList<>();
  }

  /**
   * Start listening
   */
  public void startListening() {
    String hostName = TCPContext.getHostName(thisInfo);
    int port = TCPContext.getPort(thisInfo);

    // lets connect to other
    server = new Server(config, hostName, port, looper, new ServerChannelHandler(), true);
    if (!server.start()) {
      throw new RuntimeException("Failed to start the server");
    }
  }

  /**
   * Start the connections to the servers
   * @param workerInfos information about all the workers
   */
  public void startConnections(List<NetworkInfo> workerInfos) {

    for (NetworkInfo ni : workerInfos) {
      networkInfoMap.put(ni.getProcId(), ni);
      helloSendByteBuffers.add(ByteBuffer.allocate(4));
      helloReceiveByteBuffers.add(ByteBuffer.allocate(4));

      helloSendByteBuffers.add(ByteBuffer.allocate(4));
      helloReceiveByteBuffers.add(ByteBuffer.allocate(4));
    }

    // after sync we need to connect to all the servers
    for (NetworkInfo info : workerInfos) {
      if (info.getProcId() == thisInfo.getProcId()) {
        continue;
      }

      try {
        String remoteHost = TCPContext.getHostName(info);
        int remotePort = TCPContext.getPort(info);

        Client client = new Client(remoteHost, remotePort, config,
            looper, new ClientChannelHandler());
        client.connect();
        clients.put(info.getProcId(), client);
        clientChannels.put(info.getProcId(), client.getSocketChannel());
      } catch (UnresolvedAddressException e) {
        throw new RuntimeException("Failed to create client", e);
      }
    }
  }

  /**
   * Send a buffer
   * @param buffer buffer
   * @param size size of the buffer, we assume start from 0th position
   * @param workerID the worker id
   * @param edge the edg
   * @return the reference message created
   */
  public TCPMessage iSend(ByteBuffer buffer, int size, int workerID, int edge) {
    SocketChannel ch = clientChannels.get(workerID);
    if (ch == null) {
      throw new Twister2RuntimeException("Can not send to an un-connected worker: " + workerID);
    }
    Client client = clients.get(workerID);
    return client.send(ch, buffer, size, edge);
  }

  /**
   * Receive a message
   *
   * @param buffer buffer
   * @param size size of the buffer, we assume start from 0th position
   * @param workerID the worker id
   * @param edge the edg
   * @return the reference message created
   */
  public TCPMessage iRecv(ByteBuffer buffer, int size, int workerID, int edge) {
    SocketChannel ch = serverChannels.get(workerID);
    if (ch == null) {
      throw new Twister2RuntimeException("Can not receive from an unconnected worker: " + workerID);
    }
    return server.receive(ch, buffer, size, edge);
  }

  /**
   * Progress the communication
   */
  public void progress() {
    looper.loop();
  }

  /**
   * Send a hello message after the connection established
   * @param destProcId destination id
   * @param sc channel
   */
  private void sendHelloMessage(int destProcId, SocketChannel sc) {
    ByteBuffer buffer = helloSendByteBuffers.remove(0);
    buffer.clear();
    buffer.putInt(thisInfo.getProcId());

    Client client = clients.get(destProcId);
    client.send(sc, buffer, 4, -1);
  }

  /**
   * Post the buffers to receive hello messages
   * @param sc channel
   */
  private void postHelloMessage(SocketChannel sc) {
    ByteBuffer buffer = helloReceiveByteBuffers.remove(0);
    server.receive(sc, buffer, 4, -1);
  }

  /**
   * Stop all the connections
   */
  public void stop() {
    for (Client c : clients.values()) {
      c.disconnect();
    }

    server.stop();
  }

  /**
   * Wait for handshakes to happen between servers and clients
   */
  public void waitForConnections(long timeLimit) {
    long start = System.currentTimeMillis();
    long delay = 0;

    //now wait for the handshakes to happen
    while (clientsConnected != (networkInfoMap.size() - 1)
        || (clientsCompleted != networkInfoMap.size() - 1)) {
      looper.loop();

      delay = System.currentTimeMillis() - start;
      if (delay > timeLimit) {
        throw new Twister2RuntimeException("Can not connect to all workers on the timeLimit: "
            + timeLimit + " ms");
      }
    }

    while (serverChannels.size() != networkInfoMap.size() - 1) {
      looper.loop();

      delay = System.currentTimeMillis() - start;
      if (delay > timeLimit) {
        throw new Twister2RuntimeException("Not all workers connected on the timeLimit: "
            + timeLimit + " ms");
      }
    }

    LOG.log(Level.FINEST, "Everybody connected: " + clientsConnected + " " + clientsCompleted);
  }

  public void closeConnection(int workerID) {
    Client client = clients.get(workerID);
    client.disconnect();
    clients.remove(workerID);
    networkInfoMap.remove(workerID);

    clientChannels.remove(workerID);

    SocketChannel serverChannel = serverChannels.remove(workerID);
    if (serverChannel != null) {
      try {
        serverChannel.close();
      } catch (IOException e) {
      }
    }
  }

  private class ServerChannelHandler implements ChannelHandler {

    @Override
    public void onError(SocketChannel channel, StatusCode status) {
    }

    @Override
    public void onConnect(SocketChannel channel) {
      LOG.finest("Server connected to client");
      postHelloMessage(channel);
    }

    @Override
    public void onClose(SocketChannel channel) {
      serverChannels.inverse().remove(channel);
    }

    @Override
    public void onReceiveComplete(SocketChannel channel, TCPMessage readRequest) {
      if (readRequest.getEdge() == -1) {
        ByteBuffer buffer = readRequest.getByteBuffer();
        int workerID = buffer.getInt();
        // add this to
        serverChannels.put(workerID, channel);
        LOG.finest("Server received hello message from: " + workerID);
        buffer.clear();
        helloReceiveByteBuffers.add(buffer);
        clientsConnected++;
      }
      readRequest.setComplete();
    }

    @Override
    public void onSendComplete(SocketChannel channel, TCPMessage writeRequest) {
      LOG.finest("Server send complete");
      writeRequest.setComplete();
    }
  }

  private class ClientChannelHandler implements ChannelHandler {

    @Override
    public void onError(SocketChannel channel, StatusCode status) {
    }

    @Override
    public void onConnect(SocketChannel channel) {
      LOG.finest("Client connected to server: " + channel);
      Integer workerID = clientChannels.inverse().get(channel);
      // we need to send a hello message to server
      sendHelloMessage(workerID, channel);
    }

    @Override
    public void onClose(SocketChannel channel) {
      clientChannels.inverse().remove(channel);
    }

    @Override
    public void onReceiveComplete(SocketChannel channel, TCPMessage readRequest) {
      LOG.finest("Client received message");
      readRequest.setComplete();
    }

    @Override
    public void onSendComplete(SocketChannel channel, TCPMessage writeRequest) {
      LOG.finest("Client send complete");
      writeRequest.setComplete();
      if (writeRequest.getEdge() == -1) {
        ByteBuffer buffer = writeRequest.getByteBuffer();
        buffer.clear();
        helloSendByteBuffers.add(buffer);
        clientsCompleted++;
      }
    }
  }
}
