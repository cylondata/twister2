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
package edu.iu.dsc.tws.comms.tcp.master;

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
import edu.iu.dsc.tws.comms.tcp.net.MessageHandler;
import edu.iu.dsc.tws.comms.tcp.net.Progress;
import edu.iu.dsc.tws.comms.tcp.net.Server;
import edu.iu.dsc.tws.comms.tcp.net.StatusCode;
import edu.iu.dsc.tws.comms.tcp.net.TCPContext;
import edu.iu.dsc.tws.comms.tcp.net.TCPRequest;

/**
 * The master process to synchronize the creation of connections
 */
public class TCPMaster {
  private static final Logger LOG = Logger.getLogger(TCPMaster.class.getName());

  private Server server;

  private Config config;

  private NetworkInfo thisNetworkInfo;

  private List<NetworkInfo> workerInfoList;

  private Progress progress;

  private Map<Integer, SocketChannel> channelIntegerMap = new HashMap<>();

  private List<SocketChannel> connectedChannels = new ArrayList<>();

  public TCPMaster(Config cfg, List<NetworkInfo> infoList, NetworkInfo netInfo) {
    this.config = cfg;
    this.thisNetworkInfo = netInfo;
    this.workerInfoList = infoList;
  }

  public void start() {
    String hostName = TCPContext.getHostName(thisNetworkInfo);
    int port = TCPContext.getPort(thisNetworkInfo);

    progress = new Progress();
    server = new Server(config, hostName, port, progress, new ServerEventHandler());
    server.start();

    while (true) {
      progress.loop();
    }
  }

  public void postReceives() {
    for (SocketChannel ch : connectedChannels) {
      ByteBuffer receiveBuffer = ByteBuffer.allocate(4);
      server.receive(ch, receiveBuffer, 4, -1);
    }
  }

  private void sendHelloResponse() {
    for (NetworkInfo in : workerInfoList) {
      SocketChannel channel = channelIntegerMap.get(in.getProcId());
      ByteBuffer sendBuffer = ByteBuffer.allocate(4);
      sendBuffer.putInt(0);
      server.send(channel, sendBuffer, 4, -1);
    }
  }

  private class ServerEventHandler implements MessageHandler {
    @Override
    public void onError(SocketChannel channel) {

    }

    @Override
    public void onConnect(SocketChannel channel, StatusCode status) {
      LOG.log(Level.INFO, "Connected : " + channel);
      connectedChannels.add(channel);
      if (connectedChannels.size() == workerInfoList.size()) {
        postReceives();
      }
    }

    @Override
    public void onClose(SocketChannel channel) {
      LOG.log(Level.INFO, "Connection closed: " + channel);
    }

    @Override
    public void onReceiveComplete(SocketChannel channel, TCPRequest readRequest) {
      // read the process no
      ByteBuffer buffer = readRequest.getByteBuffer();
      int processNo = buffer.getInt();
      LOG.log(Level.INFO, "Receive complete from: " + processNo);
      if (channelIntegerMap.containsKey(processNo)) {
        LOG.log(Level.WARNING, String.format("Already received worker id from %d", processNo));
      }
      channelIntegerMap.put(processNo, channel);

      if (channelIntegerMap.keySet().size() == workerInfoList.size()) {
        LOG.log(Level.INFO, "Received from all the servers, sending responses");
        sendHelloResponse();
      }
    }

    @Override
    public void onSendComplete(SocketChannel channel, TCPRequest writeRequest) {

    }
  }

  public void stop() {
    server.stop();
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

    TCPMaster master = new TCPMaster(Config.newBuilder().build(), list, networkInfo);
    master.start();
  }
}
