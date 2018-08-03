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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.common.net.NetworkInfo;
import edu.iu.dsc.tws.common.net.tcp.TCPChannel;
import edu.iu.dsc.tws.common.net.tcp.TCPContext;
import edu.iu.dsc.tws.common.net.tcp.TCPMessage;

/**
 * In this test we will use two channels to communicate
 */
public class TwoChannelTest {
  private static final int NO_OF_CHANNELS = 2;

  private List<TCPChannel> channels;

  private List<NetworkInfo> networkInfos;

  private List<ByteBuffer> buffers = new ArrayList<>();

  private Config cfg;

  private List<Thread> threads = new ArrayList<>();

  @Before
  public void setUp() throws Exception {
    cfg = Config.newBuilder().build();

    channels = new ArrayList<>();
    networkInfos = new ArrayList<>();

    for (int i = 0; i < NO_OF_CHANNELS; i++) {
      NetworkInfo info = new NetworkInfo(i);
      info.addProperty(TCPContext.NETWORK_PORT, 10025 + i);
      info.addProperty(TCPContext.NETWORK_HOSTNAME, "localhost");
      TCPChannel channel = new TCPChannel(cfg, info);
      channel.startListening();
      channels.add(channel);
      networkInfos.add(info);
    }

    // we use only one buffer
    for (int i = 0; i < NO_OF_CHANNELS; i++) {
      ByteBuffer b = ByteBuffer.allocate(1024);
      buffers.add(b);
    }

    for (int i = 0; i < NO_OF_CHANNELS; i++) {
      final int j = i;
      Thread t = new Thread(new Runnable() {
        @Override
        public void run() {
          TCPChannel channel = channels.get(j);
          channel.startConnections(networkInfos);
          channel.waitForConnections();
        }
      });
      t.start();
      threads.add(t);
    }
    // lets wait for the connections to be made
    for (int i = 0; i < NO_OF_CHANNELS; i++) {
      Thread t = threads.get(i);
      t.join();
    }
  }

  @After
  public void tearDown() throws Exception {
    for (TCPChannel channel : channels) {
      channel.stop();
    }
  }

  @Test
  public void sendMessagesTest() {
    List<TCPMessage> sends = new ArrayList<>();
    List<TCPMessage> recvs = new ArrayList<>();

    for (int i = 0; i < NO_OF_CHANNELS; i++) {
      TCPChannel channel = channels.get(i);
      for (int j = 0; j < NO_OF_CHANNELS; j++) {
        if (j != i) {
          ByteBuffer buffer = buffers.get(j);
          buffer.clear();
          TCPMessage message = channel.iRecv(buffer, 10, j, 1);
          recvs.add(message);
        }
      }
    }

    for (int i = 0; i < NO_OF_CHANNELS; i++) {
      TCPChannel channel = channels.get(i);
      for (int j = 0; j < NO_OF_CHANNELS; j++) {
        if (j != i) {
          ByteBuffer buffer = buffers.get(j);
          buffer.clear();
          buffer.put(new byte[10]);
          TCPMessage message = channel.iSend(buffer, 10, j, 1);
          sends.add(message);
        }
      }
    }

    List<Integer> completedSends = new ArrayList<>();
    List<Integer> completedRcvs = new ArrayList<>();
    while (completedRcvs.size() != NO_OF_CHANNELS || completedSends.size() != NO_OF_CHANNELS) {
      for (int i = 0; i < NO_OF_CHANNELS; i++) {
        TCPMessage sendMsg = sends.get(i);
        TCPMessage rcvMsg = recvs.get(i);

        if (sendMsg.isComplete()) {
          if (!completedSends.contains(i)) {
            completedSends.add(i);
          }
        }

        if (rcvMsg.isComplete()) {
          if (!completedRcvs.contains(i)) {
            completedRcvs.add(i);
          }
        }

        TCPChannel ch = channels.get(i);
        ch.progress();
      }
    }
  }
}
