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
package edu.iu.dsc.tws.examples.internal.rsched;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.common.net.NetworkInfo;
import edu.iu.dsc.tws.common.net.tcp.TCPChannel;
import edu.iu.dsc.tws.common.net.tcp.TCPContext;
import edu.iu.dsc.tws.common.net.tcp.TCPMessage;

public class TCPChannelExample {
  private static final Logger LOG = Logger.getLogger(TCPChannelExample.class.getName());

  private int numberOfWorkers;
  private int workerID;
  private TCPChannel channel;

  private List<NetworkInfo> networkInfos;

  private Config cfg;

  public TCPChannelExample(int numberOfWorkers, int workerID) {
    this.numberOfWorkers = numberOfWorkers;
    this.workerID = workerID;
  }

  public void setUp() throws Exception {
    cfg = Config.newBuilder().build();

    NetworkInfo selfInfo = new NetworkInfo(workerID);
    selfInfo.addProperty(TCPContext.NETWORK_PORT, 10010 + workerID);
    selfInfo.addProperty(TCPContext.NETWORK_HOSTNAME, "localhost");

    channel = new TCPChannel(cfg, selfInfo);
    channel.startListening();

    // network info of all workers except the self
    networkInfos = new ArrayList<>();
    for (int i = 0; i < numberOfWorkers; i++) {
      NetworkInfo info = new NetworkInfo(i);
      info.addProperty(TCPContext.NETWORK_PORT, 10010 + i);
      info.addProperty(TCPContext.NETWORK_HOSTNAME, "localhost");
      networkInfos.add(info);
    }

    // wait for all workers to start their tcp servers
    Thread.sleep(3000);

    // connect to all workers
    channel.startConnections(networkInfos);
    channel.waitForConnections(10000);
    LOG.info("all connected...");

    // wait for all to all connections to be established
    Thread.sleep(5000);
    if (workerID == 0) {
      throw new RuntimeException("killing intentionally");
    }

    // wait for worker 0 to die
    Thread.sleep(5000);
  }

  public void tearDown() throws Exception {
    channel.stop();
  }

  /**
   * send one tcp message to each worker and receive one tcp message from all workers
   */
  public void sendMessagesTest() {
    List<TCPMessage> sends = new ArrayList<>();
    List<TCPMessage> receives = new ArrayList<>();

    // first register receives
    for (int i = 0; i < numberOfWorkers; i++) {
      if (i == workerID) {
        continue;
      }
      ByteBuffer buffer = ByteBuffer.allocate(128);
      TCPMessage message = channel.iRecv(buffer, 10, i, 1);
      receives.add(message);
    }

    // second register sends
    for (int i = 0; i < numberOfWorkers; i++) {
      if (i == workerID) {
        continue;
      }

      String messageToSend = "hello: " + workerID;
      byte[] toSend = messageToSend.getBytes();
      ByteBuffer buffer = ByteBuffer.wrap(toSend);
      TCPMessage message = channel.iSend(buffer, toSend.length, i, 1);
      sends.add(message);
    }

    // process sends and receives
    for (int i = 0; i < numberOfWorkers * 3; i++) {
      channel.progress();
    }

    long start = System.currentTimeMillis();
    long nextLogTime = 5;
    // check results
    // process the channel if they have not completed yet
    while (!sends.isEmpty() || !receives.isEmpty()) {
      // check sends
      Iterator<TCPMessage> tcpMessageIterator = sends.iterator();
      while (tcpMessageIterator.hasNext()) {

        TCPMessage tcpMessage = tcpMessageIterator.next();
        if (tcpMessage.isComplete()) {
          LOG.info("send completed: " + new String(tcpMessage.getByteBuffer().array()));
          tcpMessageIterator.remove();
        } else if (tcpMessage.isError()) {
          String msg = new String(tcpMessage.getByteBuffer().array(), 0, tcpMessage.getLength());
          LOG.info("xxx error on sending: " + msg);
          tcpMessageIterator.remove();
        }
      }

      // check receives
      tcpMessageIterator = receives.iterator();
      while (tcpMessageIterator.hasNext()) {

        TCPMessage tcpMessage = tcpMessageIterator.next();
        if (tcpMessage.isComplete()) {
          String msg = new String(tcpMessage.getByteBuffer().array(), 0, tcpMessage.getLength());
          LOG.info("receive completed: " + msg);
          tcpMessageIterator.remove();
        } else if (tcpMessage.isError()) {
          String msg = new String(tcpMessage.getByteBuffer().array(), 0, tcpMessage.getLength());
          LOG.info("xxx error on receiving: " + msg);
          tcpMessageIterator.remove();
        }
      }
      channel.progress();

      long duration = (System.currentTimeMillis() - start) / 1000;
      if (duration > nextLogTime) {
        LOG.info("waiting messaging to complete for: " + duration + " seconds");
        nextLogTime += 5;
      }
    }
  }

  public static void main(String[] args) throws Exception {

    if (args.length != 2) {
      LOG.severe("Usage: java TCPChannelExample numberOfWorkers workerID");
    }

    int numberOfWorkers = Integer.parseInt(args[0]);
    int workerID = Integer.parseInt(args[1]);

    TCPChannelExample tcpChannelExample = new TCPChannelExample(numberOfWorkers, workerID);
    tcpChannelExample.setUp();
    tcpChannelExample.sendMessagesTest();
    tcpChannelExample.tearDown();
  }

}
