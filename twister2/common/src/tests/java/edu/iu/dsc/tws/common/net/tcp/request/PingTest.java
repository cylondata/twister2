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
package edu.iu.dsc.tws.common.net.tcp.request;

import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.Message;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.net.tcp.Progress;
import edu.iu.dsc.tws.common.net.tcp.StatusCode;
import edu.iu.dsc.tws.proto.network.Network;

public class PingTest {
  private static final Logger LOG = Logger.getLogger(PingTest.class.getName());

  private static int serverPort;
  private ExecutorService threadsPool;

  private static RRServer rrServer;

  private static RRClient rrClient;

  private static Config cfg;

  private static Progress looper;

  @BeforeClass
  public static void beforeClass() throws Exception {
  }

  @AfterClass
  public static void afterClass() throws Exception {
  }

  @Before
  public void before() throws Exception {
    threadsPool = Executors.newSingleThreadExecutor();
  }

  @After
  public void after() throws Exception {
    threadsPool.shutdownNow();
    threadsPool = null;
  }

  @Test
  public void testStart() throws Exception {
    long start = System.currentTimeMillis();
    looper = new Progress();

    runServer();
    runClient();

    boolean disconnected = false;
    while (true) {
      looper.loop();
      if (System.currentTimeMillis() - start > 1000) {
        if (!disconnected) {
          rrClient.disconnect();
          disconnected = true;
        }
      }

      if (System.currentTimeMillis() - start > 2000) {
        break;
      }
    }
    Thread.sleep(1000);
  }

  private void runServer() {
    rrServer = new RRServer(cfg, "localhost", 23456, looper, 1,
        new ServerConnectHandler());
    rrServer.registerRequestHandler(Network.Ping.newBuilder(), new ServerPingHandler());
    rrServer.start();
  }

  public class ServerPingHandler implements MessageHandler {
    @Override
    public void onMessage(RequestID id, int workerId, Message message) {
      LOG.log(Level.INFO, "Received message to server");
      rrServer.sendResponse(id, Network.Ping.newBuilder().setPingMessage("Hello").build());
    }
  }

  public class ClientPingHandler implements MessageHandler {
    @Override
    public void onMessage(RequestID id, int workerId, Message message) {
      if (message instanceof Network.Ping) {
        System.out.println("Received ping response message");
        LOG.log(Level.INFO, "Received message");
      }
    }
  }

  private void runClient() {
    rrClient = new RRClient("localhost", 23456, cfg, looper, 2,
        new ClientConnectHandler());
    rrClient.registerResponseHandler(Network.Ping.newBuilder(), new ClientPingHandler());
    rrClient.connect();
  }

  public class ClientConnectHandler implements ConnectHandler {
    @Override
    public void onError(SocketChannel channel) {

    }

    @Override
    public void onConnect(SocketChannel channel, StatusCode status) {
      Network.Ping ping = Network.Ping.newBuilder().setPingMessage("Hello").build();
      rrClient.sendRequest(ping);
    }

    @Override
    public void onClose(SocketChannel channel) {

    }
  }

  public class ServerConnectHandler implements ConnectHandler {
    @Override
    public void onError(SocketChannel channel) {
    }

    @Override
    public void onConnect(SocketChannel channel, StatusCode status) {
    }

    @Override
    public void onClose(SocketChannel channel) {
    }
  }
}
