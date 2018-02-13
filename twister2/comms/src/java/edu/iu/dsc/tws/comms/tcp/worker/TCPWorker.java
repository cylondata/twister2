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
package edu.iu.dsc.tws.comms.tcp.worker;

import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.logging.Level;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.core.NetworkInfo;
import edu.iu.dsc.tws.comms.tcp.net.Client;
import edu.iu.dsc.tws.comms.tcp.net.MessageHandler;
import edu.iu.dsc.tws.comms.tcp.net.Progress;
import edu.iu.dsc.tws.comms.tcp.net.StatusCode;
import edu.iu.dsc.tws.comms.tcp.net.TCPContext;
import edu.iu.dsc.tws.comms.tcp.net.TCPRequest;

public class TCPWorker {
  private static final Logger LOG = Logger.getLogger(TCPWorker.class.getName());

  private NetworkInfo masterInfo;

  private Config config;

  private Client masterClient;

  private Progress progress;

  private ByteBuffer sendBuffer;

  private SocketChannel clientSocketChannel;

  private boolean isReady = false;

  public TCPWorker(Config cfg, NetworkInfo master) {
    this.config = cfg;
    this.masterInfo = master;
  }

  public void start() {
    String hostName = TCPContext.getHostName(masterInfo);
    int port = TCPContext.getPort(masterInfo);
    sendBuffer = ByteBuffer.allocate(128);

    sendBuffer.putInt(masterInfo.getProcId());

    // create the progress
    progress = new Progress();

    // now lets start a connection to master
    masterClient = new Client(hostName, port, config, progress, new MasterEventHandler());
    masterClient.connect();
  }

  public void waitForSync() {
    while (!isReady) {
      progress.loop();
    }
  }

  public void stop() {
    masterClient.disconnect();
  }

  public void sendAndPost() {
    ByteBuffer byteBuffer = ByteBuffer.allocate(4);
    masterClient.receive(clientSocketChannel, byteBuffer, 4, -1);

    TCPRequest request = masterClient.send(clientSocketChannel, sendBuffer, 4, -1);
    if (request == null) {
      LOG.log(Level.WARNING, "Message sending not accepted");
    }
  }

  private class MasterEventHandler implements MessageHandler {
    @Override
    public void onError(SocketChannel channel) {
      LOG.log(Level.SEVERE, "Error happened on connection: " + channel);
    }

    @Override
    public void onConnect(SocketChannel channel, StatusCode status) {
      LOG.log(Level.INFO, "Client connected to master: " + channel);
      clientSocketChannel = channel;
      sendAndPost();
    }

    @Override
    public void onClose(SocketChannel channel) {
      LOG.log(Level.INFO, "Connection closed: " + channel);
    }

    @Override
    public void onReceiveComplete(SocketChannel channel, TCPRequest readRequest) {
      LOG.log(Level.INFO, "Received the hello response");
      isReady = true;
    }

    @Override
    public void onSendComplete(SocketChannel channel, TCPRequest writeRequest) {

    }
  }
}
