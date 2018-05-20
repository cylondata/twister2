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
package edu.iu.dsc.tws.common.net.tcp.server;

import java.nio.channels.SocketChannel;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import com.google.protobuf.Message;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.net.tcp.ChannelHandler;
import edu.iu.dsc.tws.common.net.tcp.Progress;
import edu.iu.dsc.tws.common.net.tcp.RequestID;
import edu.iu.dsc.tws.common.net.tcp.ResponseHandler;
import edu.iu.dsc.tws.common.net.tcp.Server;
import edu.iu.dsc.tws.common.net.tcp.StatusCode;
import edu.iu.dsc.tws.common.net.tcp.TCPMessage;

public class RRServer {
  private static final Logger LOG = Logger.getLogger(RRServer.class.getName());

  private Server server;

  /**
   * Client id to channel
   */
  private Map<Integer, SocketChannel> socketChannels = new HashMap<>();

  /**
   * Keep track of the request
   */
  private Map<RequestID, ResponseHandler> requests = new HashMap<>();

  /**
   * Keep track of the response handler using protocol buffer message types
   */
  private Map<Message.Builder, ResponseHandler> staticResponseHandlers = new HashMap<>();

  public RRServer(Config cfg, String host, int port, Progress loop) {
    server = new Server(cfg, host, port, loop, new Handler());
  }

  public void registerRequestHandler(Message.Builder builder, ResponseHandler handler) {
    staticResponseHandlers.put(builder, handler);
  }

  public void sendResponse(RequestID requestID, Message message) {
  }

  private class Handler implements ChannelHandler {
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
    public void onReceiveComplete(SocketChannel channel, TCPMessage readRequest) {

    }

    @Override
    public void onSendComplete(SocketChannel channel, TCPMessage writeRequest) {

    }
  }
}
