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

  private TCPWorker worker;

  private Map<Integer, Client> clients;

  private Progress looper;

  private Config config;

  private List<NetworkInfo> networkInfos;

  private NetworkInfo thisInfo;

  private NetworkInfo masterInfo;

  private Map<Integer, NetworkInfo> networkInfoMap;

  private Map<Integer, SocketChannel> clientChannel;

  private Map<Integer, SocketChannel> serverChannel;


  public TCPChannel(Config cfg, List<NetworkInfo> workerInfo, NetworkInfo info, NetworkInfo mInfo) {
    config = cfg;
    networkInfos = workerInfo;
    thisInfo = info;
    masterInfo = mInfo;

    clientChannel = new HashMap<>();
    serverChannel = new HashMap<>();
    clients = new HashMap<>();
    looper = new Progress();

    networkInfoMap = new HashMap<>();
    for (NetworkInfo ni : workerInfo) {
      networkInfoMap.put(ni.getProcId(), ni);
    }
  }

  public void start() {
    String hostName = TCPContext.getHostName(thisInfo);
    int port = TCPContext.getPort(thisInfo);

    looper = new Progress();

    // lets connect to other
    server = new Server(config, hostName, port, looper, new ChannelServerMessageHandler());
    server.start();

    // now we need to sync the workers to start the servers
    worker = new TCPWorker(config, masterInfo);
    worker.start();
    worker.waitForSync();

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
    }
  }

  public TCPRequest iSend(ByteBuffer buffer, int size, int procId, int edge) {
    SocketChannel ch = clientChannel.get(procId);
    if (ch == null) {
      LOG.log(Level.INFO, "Cannot send on an un-connected channel");
      return null;
    }
    return server.send(ch, buffer, size, edge);
  }

  public TCPRequest iRecv(ByteBuffer buffer, int size, int procId, int edge) {
    SocketChannel ch = serverChannel.get(procId);
    if (ch == null) {
      LOG.log(Level.INFO, "Cannot receive on an un-connected channel");
      return null;
    }
    return server.receive(ch, buffer, size, edge);
  }

  public void progress() {
    looper.loop();
  }

  private class ChannelServerMessageHandler implements MessageHandler {

    @Override
    public void onError(SocketChannel channel) {

    }

    @Override
    public void onConnect(SocketChannel channel, StatusCode status) {

    }

    @Override
    public void onClose(SocketChannel channel) {

    }

    @Override
    public void onReceiveComplete(SocketChannel channel, TCPReadRequest readRequest) {

    }

    @Override
    public void onSendComplete(SocketChannel channel, TCPWriteRequest writeRequest) {

    }
  }

  private class ClientChannelMessageHandler implements MessageHandler {

    @Override
    public void onError(SocketChannel channel) {
    }

    @Override
    public void onConnect(SocketChannel channel, StatusCode status) {

    }

    @Override
    public void onClose(SocketChannel channel) {

    }

    @Override
    public void onReceiveComplete(SocketChannel channel, TCPReadRequest readRequest) {

    }

    @Override
    public void onSendComplete(SocketChannel channel, TCPWriteRequest writeRequest) {

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
      NetworkInfo info = new NetworkInfo(procId);
      info.addProperty(TCPContext.NETWORK_HOSTNAME, "localhost");
      info.addProperty(TCPContext.NETWORK_PORT, 8765 + i);
      list.add(info);
    }

    TCPChannel master = new TCPChannel(Config.newBuilder().build(), list,
        list.get(procId), networkInfo);
    master.start();
  }
}
