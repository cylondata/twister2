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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.core.NetworkInfo;

public class TCPChannel {
  private Server server;

  private Map<Integer, Client> clients;

  private Progress looper;

  private Config config;

  private List<NetworkInfo> networkInfos;

  private Map<Integer, NetworkInfo> networkInfoMap;

  private int processId;

  public TCPChannel(Config cfg,  Progress loop, List<NetworkInfo> info, int procId) {
    config = cfg;
    looper = loop;
    networkInfos = info;

    networkInfoMap = new HashMap<>();
    for (NetworkInfo ni : info) {
      networkInfoMap.put(ni.getProcId(), ni);
    }
  }

  public TCPRequest iSend(ByteBuffer buffer, int size, int procId, int edge) {
    return null;
  }

  public TCPRequest iRecv(ByteBuffer buffer, int size, int procId, int edge) {
    return null;
  }

  private void startServers() {

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
}
