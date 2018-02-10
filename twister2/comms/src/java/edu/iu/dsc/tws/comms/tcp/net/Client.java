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
import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SocketChannel;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;

public class Client implements SelectHandler {
  private static final Logger LOG = Logger.getLogger(Client.class.getName());

  private SocketChannel socketChannel;

  private InetSocketAddress address;

  private Config config;

  private Progress progress;

  private Channel channel;

  private boolean isConnected;

  private MessageHandler messageHandler;

  public Client(String host, int port, Config cfg, Progress looper, MessageHandler handler) {
    address = new InetSocketAddress(host, port);
    config = cfg;
    isConnected = false;
    progress = looper;
    messageHandler = handler;
  }

  public boolean connect() {
    try {
      socketChannel = SocketChannel.open();
      socketChannel.configureBlocking(false);
      socketChannel.socket().setTcpNoDelay(true);

      LOG.log(Level.INFO, "Connecting to endpoint: " + address);
      if (socketChannel.connect(address)) {
        handleConnect(socketChannel);
      } else {
        progress.registerConnect(socketChannel, this);
      }
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Error connecting to remote endpoint: " + address, e);
      return false;
    }

    return true;
  }

  public TCPRequest send(SocketChannel sc, ByteBuffer buffer, int size, int edge) {
    if (sc != socketChannel) {
      return null;
    }

    if (!isConnected) {
      return null;
    }

    TCPRequest request = new TCPRequest(buffer, edge, size);
    channel.addWriteRequest(request);

    return request;
  }

  public TCPRequest receive(SocketChannel sc, ByteBuffer buffer, int size, int edge) {
    if (sc != socketChannel) {
      return null;
    }

    if (!isConnected) {
      return null;
    }

    TCPRequest request = new TCPRequest(buffer, edge, size);
    channel.addReadRequest(request);

    return request;
  }

  public void disconnect() {
    if (!isConnected) {
      return;
    }

    channel.forceFlush();

    try {
      socketChannel.close();
      // we call the onclose with null value
      messageHandler.onClose(null);
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to stop Client", e);
    }
  }

  @Override
  public void handleRead(SelectableChannel ch) {
    channel.read();
  }

  @Override
  public void handleWrite(SelectableChannel ch) {
    channel.write();
  }

  @Override
  public void handleAccept(SelectableChannel ch) {
    throw new RuntimeException("Client cannot accept connections");
  }

  @Override
  public void handleConnect(SelectableChannel selectableChannel) {
    try {
      if (socketChannel.finishConnect()) {
        progress.unregisterConnect(selectableChannel);
      }
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to FinishConnect to endpoint: " + address, e);
      messageHandler.onConnect(socketChannel, StatusCode.ERROR_CONN);
      return;
    }
    channel = new Channel(config, progress, this, socketChannel, messageHandler);
    channel.enableReading();
    channel.enableWriting();

    isConnected = true;
    messageHandler.onConnect(socketChannel, StatusCode.SUCCESS);
  }

  @Override
  public void handleError(SelectableChannel ch) {
    channel.clear();
    progress.removeAllInterest(ch);

    try {
      ch.close();
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to close connection in handleError", e);
    }

    isConnected = false;
  }

  SocketChannel getSocketChannel() {
    return socketChannel;
  }
}
