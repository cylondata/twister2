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
package edu.iu.dsc.tws.comms.tcp.net;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.core.NetworkInfo;
import edu.iu.dsc.tws.comms.tcp.worker.TCPWorker;

public class TCPChannel {
  private static final Logger LOG = Logger.getLogger(TCPChannel.class.getName());

  private Server server;

  private Map<Integer, Client> clients;

  private Progress looper;

  private Config config;

  private List<NetworkInfo> networkInfos;

  private NetworkInfo thisInfo;

  private Map<Integer, NetworkInfo> networkInfoMap;

  private Map<Integer, SocketChannel> clientChannel;

  private Map<SocketChannel, Integer> invertedClientChannels;
  private Map<SocketChannel, Integer> invertedServerChannels;

  private Map<Integer, SocketChannel> serverChannel;

  private List<SocketChannel> serverSocketChannels;

  private List<SocketChannel> clientSocketChannels;

  // we use a pre-allocated set of buffers to send the hello messages to
  // the servers connected to by the client, so each client will send this message
  private List<ByteBuffer> helloSendByteBuffers;
  private List<ByteBuffer> helloReceiveByteBuffers;

  private int clientsCompleted = 0;
  private int clientsConnected = 0;

  public TCPChannel(Config cfg, NetworkInfo info) {
    config = cfg;
    thisInfo = info;

    clientChannel = new HashMap<>();
    serverChannel = new HashMap<>();
    invertedClientChannels = new HashMap<>();
    invertedServerChannels = new HashMap<>();

    clients = new HashMap<>();
    serverSocketChannels = new ArrayList<>();
    clientSocketChannels = new ArrayList<>();
    looper = new Progress();

    networkInfoMap = new HashMap<>();
    helloSendByteBuffers = new ArrayList<>();
    helloReceiveByteBuffers = new ArrayList<>();
  }

  /**
   * Start
   */
  public void startFirstPhase() {
    String hostName = TCPContext.getHostName(thisInfo);
    int port = TCPContext.getPort(thisInfo);

    looper = new Progress();

    // lets connect to other
    server = new Server(config, hostName, port, looper, new ChannelServerMessageHandler());
    server.start();
  }

  public void startSecondPhase(List<NetworkInfo> workerInfo, NetworkInfo updatedThisInfo) {
    this.networkInfos = workerInfo;

    for (NetworkInfo ni : workerInfo) {
      networkInfoMap.put(ni.getProcId(), ni);
      helloSendByteBuffers.add(ByteBuffer.allocate(4));
      helloReceiveByteBuffers.add(ByteBuffer.allocate(4));

      helloSendByteBuffers.add(ByteBuffer.allocate(4));
      helloReceiveByteBuffers.add(ByteBuffer.allocate(4));
    }

    // after sync we need to connect to all the servers
    for (NetworkInfo info : networkInfos) {
      if (info.getProcId() == thisInfo.getProcId()) {
        continue;
      }

      String remoteHost = TCPContext.getHostName(info);
      int remotePort = TCPContext.getPort(info);

      Client client = new Client(remoteHost, remotePort, config,
          looper, new ClientChannelMessageHandler());
      client.connect();
      clients.put(info.getProcId(), client);
      invertedClientChannels.put(client.getSocketChannel(), info.getProcId());
    }

    //now wait for the handshakes to happen
    while (clientsConnected != (networkInfos.size() - 1)
        || (clientsCompleted != networkInfos.size() - 1)) {
      looper.loop();
    }

    LOG.log(Level.INFO, "Everybody connected: " + clientsConnected + " " + clientsCompleted);
  }

  public TCPRequest iSend(ByteBuffer buffer, int size, int procId, int edge) {
    SocketChannel ch = clientChannel.get(procId);
    if (ch == null) {
      LOG.log(Level.INFO, "Cannot send on an un-connected channel to: " + procId);
      return null;
    }
    Client client = clients.get(procId);
    return client.send(ch, buffer, size, edge);
  }

  public TCPRequest iRecv(ByteBuffer buffer, int size, int procId, int edge) {
    SocketChannel ch = serverChannel.get(procId);
    if (ch == null) {
      LOG.log(Level.INFO, "Cannot receive on an un-connected channel to: " + procId);
      return null;
    }
    return server.receive(ch, buffer, size, edge);
  }

  public void progress() {
    looper.loop();
  }

  private void sendHelloMessage(int destProcId, SocketChannel sc) {
    ByteBuffer buffer = helloSendByteBuffers.remove(0);
    buffer.putInt(thisInfo.getProcId());

    Client client = clients.get(destProcId);
    client.send(sc, buffer, 4, -1);
  }

  private void postHelloMessage(SocketChannel sc) {
    ByteBuffer buffer = helloReceiveByteBuffers.remove(0);
    server.receive(sc, buffer, 4, -1);
  }

  public void stop() {
    for (Client c : clients.values()) {
      c.disconnect();
    }

    server.stop();
  }

  private class ChannelServerMessageHandler implements MessageHandler {

    @Override
    public void onError(SocketChannel channel) {
    }

    @Override
    public void onConnect(SocketChannel channel, StatusCode status) {
      LOG.log(Level.INFO, "Server connected to client");
      serverSocketChannels.add(channel);
      postHelloMessage(channel);
    }

    @Override
    public void onClose(SocketChannel channel) {
      if (!serverSocketChannels.remove(channel)) {
        LOG.warning("Removing an un-exsting channel: " + channel);
      }
    }

    @Override
    public void onReceiveComplete(SocketChannel channel, TCPRequest readRequest) {
      if (readRequest.getEdge() == -1) {
        ByteBuffer buffer = readRequest.getByteBuffer();
        int destProc = buffer.getInt();
        // add this to
        invertedServerChannels.put(channel, destProc);
        serverChannel.put(destProc, channel);
        LOG.log(Level.INFO, "Server received hello message from: " + destProc);
        buffer.clear();
        helloReceiveByteBuffers.add(buffer);
        clientsConnected++;
      }
      readRequest.setComplete(true);
    }

    @Override
    public void onSendComplete(SocketChannel channel, TCPRequest writeRequest) {
      LOG.log(Level.INFO, "Server send complete");
      writeRequest.setComplete(true);
    }
  }

  private class ClientChannelMessageHandler implements MessageHandler {

    @Override
    public void onError(SocketChannel channel) {
    }

    @Override
    public void onConnect(SocketChannel channel, StatusCode status) {
      LOG.log(Level.INFO, "Client connected to server: " + channel);
      clientSocketChannels.add(channel);
      Integer key = invertedClientChannels.get(channel);
      // we need to send a hello message to server
      sendHelloMessage(key, channel);
      clientChannel.put(key, channel);
    }

    @Override
    public void onClose(SocketChannel channel) {
      if (!clientSocketChannels.remove(channel)) {
        LOG.warning("Removing an un-exsting channel: " + channel);
      }
    }

    @Override
    public void onReceiveComplete(SocketChannel channel, TCPRequest readRequest) {
      LOG.log(Level.INFO, "Client received message");
      readRequest.setComplete(true);
    }

    @Override
    public void onSendComplete(SocketChannel channel, TCPRequest writeRequest) {
      LOG.log(Level.INFO, "Client send complete");
      writeRequest.setComplete(true);
      if (writeRequest.getEdge() == -1) {
        ByteBuffer buffer = writeRequest.getByteBuffer();
        buffer.clear();
        helloSendByteBuffers.add(buffer);
        clientsCompleted++;
      }
    }
  }

  public static void main(String[] args) {
    int noOfProcs = Integer.parseInt(args[1]);
    int procId = Integer.parseInt(args[0]);

    NetworkInfo networkInfo = new NetworkInfo(procId);
    networkInfo.addProperty(TCPContext.NETWORK_HOSTNAME, "localhost");
    networkInfo.addProperty(TCPContext.NETWORK_PORT, 8764);

    List<NetworkInfo> list = new ArrayList<>();
    for (int i = 0; i < noOfProcs; i++) {
      NetworkInfo info = new NetworkInfo(i);
      info.addProperty(TCPContext.NETWORK_HOSTNAME, "localhost");
      info.addProperty(TCPContext.NETWORK_PORT, 8765 + i);
      list.add(info);
    }

    TCPChannel master = new TCPChannel(Config.newBuilder().build(), list.get(procId));
    master.startFirstPhase();

    TCPWorker worker = new TCPWorker(Config.newBuilder().build(), networkInfo);
    worker.start();
    worker.waitForSync();
    LOG.log(Level.INFO, "Workers are synced..");

    master.startSecondPhase(list, networkInfo);

    int destProcId = 0;
    if (procId == 0) {
      destProcId = 1;
    }

    List<TCPRequest> readRequests = new ArrayList<>();
    List<TCPRequest> writeRequests = new ArrayList<>();
    final int messages = 5;
    // now lets send 5 messages
    for (int i = 0; i < messages; i++) {
      if (destProcId == 0) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(8);
        byteBuffer.putInt(1);
        byteBuffer.putInt(2);
        TCPRequest write = master.iSend(byteBuffer, 8, destProcId, 1);
        writeRequests.add(write);
      } else {
        ByteBuffer receiveBuffer = ByteBuffer.allocate(8);
        TCPRequest read = master.iRecv(receiveBuffer, 8, destProcId, 1);
        readRequests.add(read);
      }
    }

    int completed = 0;
    int writeCOmpeted = 0;
    do {
      master.progress();
      if (destProcId == 0) {
        Iterator<TCPRequest> wItr = writeRequests.iterator();
        while (wItr.hasNext()) {
          TCPRequest w = wItr.next();
          if (w.isComplete()) {
            LOG.info("Write complete : " + writeCOmpeted);
            writeCOmpeted++;
            wItr.remove();
          }
        }
      } else {
        Iterator<TCPRequest> rItr = readRequests.iterator();
        while (rItr.hasNext()) {
          TCPRequest r = rItr.next();
          if (r.isComplete()) {
            ByteBuffer buffer = r.getByteBuffer();
//            LOG.info("Size: " + buffer.remaining());
            int first = buffer.getInt();
            int second = buffer.getInt();
            LOG.info("Read complete : " + completed + " " + first + " " + second);
            completed++;
            rItr.remove();
          }
        }
      }
    } while (completed != messages && writeCOmpeted != messages);

    try {
      Thread.sleep(60000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    worker.stop();
    master.stop();
  }
}
