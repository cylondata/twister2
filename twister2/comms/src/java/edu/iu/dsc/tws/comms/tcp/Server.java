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
import java.net.SocketAddress;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Server {
  private static Logger LOG = Logger.getLogger(Server.class.getName());

  private ServerSocketChannel serverSocketChannel;
  private InetSocketAddress address;

  private Map<SocketChannel, Channel> connectedChannels = new HashMap<>();

  public boolean start() {
    try {
      serverSocketChannel = ServerSocketChannel.open();
      serverSocketChannel.configureBlocking(false);
      serverSocketChannel.socket().bind(address);
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

      connections.getValue().clear();
    }


    try {
      serverSocketChannel.close();
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to close server", e);
    }
  }

}
