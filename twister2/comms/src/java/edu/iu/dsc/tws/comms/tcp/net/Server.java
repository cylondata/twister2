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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;

public class Server implements SelectHandler {
  private static final Logger LOG = Logger.getLogger(Server.class.getName());

  private ServerSocketChannel serverSocketChannel;
  private InetSocketAddress address;

  private Map<SocketChannel, Channel> connectedChannels = new HashMap<>();

  private Config config;

  private Progress progress;

  private MessageHandler messageHandler;

  public Server(Config cfg, String host, int port, Progress loop,
                MessageHandler msgHandler) {
    this.config = cfg;
    this.progress = loop;
    address = new InetSocketAddress(host, port);
    this.messageHandler = msgHandler;
  }

  public boolean start() {
    try {
      serverSocketChannel = ServerSocketChannel.open();
      serverSocketChannel.configureBlocking(false);
      serverSocketChannel.socket().bind(address);
      progress.registerAccept(serverSocketChannel, this);
      return true;
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to start server", e);
      return false;
    }
  }

  // Stop the HeronServer and clean relative staff
  public void stop() {
    if (serverSocketChannel == null || !serverSocketChannel.isOpen()) {
      LOG.info("Fail to stop server; not yet open.");
      return;
    }
    for (Map.Entry<SocketChannel, Channel> connections : connectedChannels.entrySet()) {
      SocketChannel channel = connections.getKey();
      progress.removeAllInterest(channel);

      connections.getValue().clear();
    }

    try {
      serverSocketChannel.close();
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to close server", e);
    }
  }

  public TCPRequest send(SocketChannel sc, ByteBuffer buffer, int size, int edge) {
    Channel channel = connectedChannels.get(sc);
    if (channel == null) {
      return null;
    }

    TCPRequest request = new TCPRequest(buffer, edge, size);
    channel.addWriteRequest(request);

    return request;
  }

  public TCPRequest receive(SocketChannel sc, ByteBuffer buffer, int size, int edge) {
    Channel channel = connectedChannels.get(sc);
    if (channel == null) {
      return null;
    }

    TCPRequest request = new TCPRequest(buffer, edge, size);
    channel.addReadRequest(request);

    return request;
  }

  @Override
  public void handleRead(SelectableChannel ch) {
    Channel channel = connectedChannels.get(ch);
    if (channel != null) {
      channel.read();
    } else {
      LOG.warning("Un-expected channel ready for read");
    }
  }

  @Override
  public void handleWrite(SelectableChannel ch) {
    Channel channel = connectedChannels.get(ch);
    if (channel != null) {
      channel.write();
    } else {
      LOG.warning("Un-expected channel ready for write");
    }
  }

  @Override
  public void handleAccept(SelectableChannel ch) {
    try {
      SocketChannel socketChannel = serverSocketChannel.accept();
      if (socketChannel != null) {
        socketChannel.configureBlocking(false);
        socketChannel.socket().setTcpNoDelay(true);

        Channel channel = new Channel(config, progress, this, socketChannel, messageHandler);
        channel.enableReading();
        channel.enableWriting();
        connectedChannels.put(socketChannel, channel);

        messageHandler.onConnect(socketChannel, StatusCode.SUCCESS);
      }
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Error while accepting a new connection ", e);
    }
  }

  @Override
  public void handleConnect(SelectableChannel ch) {
    throw new RuntimeException("Server not supported in server");
  }

  @Override
  public void handleError(SelectableChannel ch) {
    SocketAddress channelAddress = ((SocketChannel) ch).socket().getRemoteSocketAddress();
    LOG.log(Level.INFO, "Connection is closed: " + channelAddress);
    Channel channel = connectedChannels.get(ch);
    if (channel == null) {
      LOG.warning("Error occurred in non-existing channel");
      return;
    }
    channel.clear();
    progress.removeAllInterest(ch);
    try {
      ch.close();
    } catch (IOException e) {
      LOG.warning("Error closing conection in error handler");
    }
    connectedChannels.remove(ch);
  }
}
