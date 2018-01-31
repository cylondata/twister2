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
package edu.iu.dsc.tws.comms.tcp;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.SocketChannel;
import java.time.Duration;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;

public class Client {
  private static final Logger LOG = Logger.getLogger(Client.class.getName());

  private SocketChannel clientChannel;

  private InetSocketAddress address;

  private boolean connected;

  private Config config;

  public Client(String host, int port, Config cfg) {
    address = new InetSocketAddress(host, port);
    config = cfg;
    connected = false;
  }

  public void start() {
    try {
      clientChannel = SocketChannel.open();
      clientChannel.configureBlocking(false);

      clientChannel.socket().setTcpNoDelay(true);
      clientChannel.socket().setSendBufferSize(
          TCPContext.getSocketSendBufferSize(config, 1024));
      clientChannel.socket().setReceiveBufferSize(
          TCPContext.getSocketReceivedBufferSize(config, 1024));

      LOG.log(Level.INFO,"Connecting to endpoint: " + address);
      if (clientChannel.connect(address)) {
        handleConnect(socketChannel);
      } else {
        nioLooper.registerConnect(socketChannel, this);
      }
    } catch (IOException e) {
      // Call onConnect() with CONNECT_ERROR
      LOG.log(Level.SEVERE, "Error connecting to remote endpoint: " + address, e);
      Runnable r = new Runnable() {
        public void run() {
          onConnect(StatusCode.CONNECT_ERROR);
        }
      };
      nioLooper.registerTimerEvent(Duration.ZERO, r);
    }
  }

  public void stop() {
    if (!connected) {
      return;
    }

    try {
      clientChannel.close();
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to stop Client", e);
    }
  }
}
