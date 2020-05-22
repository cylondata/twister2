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
package edu.iu.dsc.tws.comms.ucx;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import edu.iu.dsc.tws.api.comms.channel.ChannelListener;
import edu.iu.dsc.tws.api.comms.messaging.ChannelMessage;
import edu.iu.dsc.tws.api.comms.messaging.MessageHeader;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.comms.packing.DataBuffer;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.exceptions.JobFaultyException;
import edu.iu.dsc.tws.api.exceptions.TimeoutException;
import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;
import edu.iu.dsc.tws.api.resource.IWorkerController;
import edu.iu.dsc.tws.comms.dfw.InMessage;
import edu.iu.dsc.tws.comms.dfw.OutMessage;
import edu.iu.dsc.tws.comms.dfw.io.DataDeserializer;
import edu.iu.dsc.tws.comms.dfw.io.DataSerializer;
import edu.iu.dsc.tws.proto.jobmaster.JobMasterAPI;

public class TWSUCXChannelTest {

  private static final Logger LOG = Logger.getLogger(TWSUCXChannelTest.class.getName());

  private class MockWorkerController implements IWorkerController {

    private int workerId;
    private List<JobMasterAPI.WorkerInfo> workerInfos;
    private CyclicBarrier cyclicBarrier;

    MockWorkerController(int id, List<JobMasterAPI.WorkerInfo> workerInfos,
                         CyclicBarrier cyclicBarrier) {
      this.workerId = id;
      this.workerInfos = workerInfos;
      this.cyclicBarrier = cyclicBarrier;
    }

    @Override
    public JobMasterAPI.WorkerInfo getWorkerInfo() {
      return workerInfos.get(workerId);
    }

    @Override
    public JobMasterAPI.WorkerInfo getWorkerInfoForID(int id) {
      return workerInfos.get(id);
    }

    @Override
    public int getNumberOfWorkers() {
      return workerInfos.size();
    }

    @Override
    public List<JobMasterAPI.WorkerInfo> getJoinedWorkers() {
      return workerInfos;
    }

    @Override
    public List<JobMasterAPI.WorkerInfo> getAllWorkers() throws TimeoutException {
      return workerInfos;
    }

    @Override
    public void waitOnBarrier() throws TimeoutException {
      try {
        this.cyclicBarrier.await();
      } catch (InterruptedException | BrokenBarrierException e) {
        throw new Twister2RuntimeException("Failed on barrier");
      }
    }

    @Override
    public void waitOnBarrier(long timeLimit) throws TimeoutException, JobFaultyException {
      waitOnBarrier();
    }

    @Override
    public void waitOnInitBarrier() throws TimeoutException {
      waitOnBarrier();
    }
  }

  private List<MockWorkerController> workerControllers = new ArrayList<>();

  @Before
  public void setup() {
    List<JobMasterAPI.WorkerInfo> workerInfos = new ArrayList<>();
    CyclicBarrier cyclicBarrier = new CyclicBarrier(2);
    for (int i = 0; i < 2; i++) {
      workerInfos.add(
          JobMasterAPI.WorkerInfo.newBuilder()
              .setWorkerID(i)
              .setPort(8000 + i)
              .setWorkerIP("0.0.0.0")
              .build()
      );
      this.workerControllers.add(new MockWorkerController(i, workerInfos, cyclicBarrier));
    }
  }

  @Test
  public void ucxTest() throws InterruptedException {

    final int dataSize = 25;

    final int[] data = new int[dataSize];
    for (int i = 0; i < dataSize; i++) {
      data[i] = i;
    }

    new Thread(() -> {
      TWSUCXChannel sender = new TWSUCXChannel(
          Config.newBuilder().build(),
          workerControllers.get(0)
      );


      OutMessage outMessage = new OutMessage(0, 1, -1, 10, 0, null,
          null, MessageTypes.INTEGER_ARRAY, null, null, data);

      Queue<DataBuffer> sendingBuffers = new ConcurrentLinkedQueue<>();
      sendingBuffers.add(new DataBuffer(sender.createBuffer(50)));
      sendingBuffers.add(new DataBuffer(sender.createBuffer(50)));

      DataSerializer dataSerializer = new DataSerializer();
      dataSerializer.init(Config.newBuilder().build(), sendingBuffers);

      AtomicInteger sendingCount = new AtomicInteger();

      AtomicBoolean progress = new AtomicBoolean(true);
      while (progress.get()) {
        ChannelMessage channelMessage = dataSerializer.build(data, outMessage);
        if (channelMessage != null) {
          sender.sendMessage(1, channelMessage, new ChannelListener() {
            @Override
            public void onReceiveComplete(int id, int stream, DataBuffer message) {

            }

            @Override
            public void onSendComplete(int id, int stream, ChannelMessage message) {
              message.getBuffers().forEach(dataBuffer -> {
                dataBuffer.setSize(0);
                dataBuffer.getByteBuffer().clear();
              });
              sendingBuffers.addAll(message.getBuffers());
              progress.set(!outMessage.getSendState().equals(OutMessage.SendState.SERIALIZED));
              //LOG.info("Sending Count : " + sendingCount.incrementAndGet());
            }
          });
        }
        sender.progress();
      }
      LOG.info("Exiting send loop...");
    }).start();


    TWSUCXChannel receiver = new TWSUCXChannel(
        Config.newBuilder().build(),
        workerControllers.get(1)
    );

    Queue<DataBuffer> recvBuffers = new ConcurrentLinkedQueue<>();
    recvBuffers.add(new DataBuffer(receiver.createBuffer(50)));
    recvBuffers.add(new DataBuffer(receiver.createBuffer(50)));

    DataDeserializer dataDeserializer = new DataDeserializer();
    dataDeserializer.init(Config.newBuilder().build());

    AtomicInteger recvCount = new AtomicInteger(0);
    AtomicBoolean received = new AtomicBoolean(false);

    final int[][] receivedData = new int[1][dataSize];

    receiver.receiveMessage(0, 0, 1, new ChannelListener() {

      private InMessage inMessage;

      @Override
      public void onReceiveComplete(int id, int stream, DataBuffer message) {
        if (recvCount.incrementAndGet() == 1) {
          MessageHeader messageHeader = dataDeserializer.buildHeader(message, 1);
          this.inMessage = new InMessage(id, MessageTypes.INTEGER_ARRAY, channelMessage -> {
            LOG.info("Release called");
          }, messageHeader);
        }
        LOG.info("Received : " + recvCount.get());
        if (this.inMessage.addBufferAndCalculate(message)) {
          LOG.info("Built");
          dataDeserializer.build(this.inMessage, 1);
          receivedData[0] = (int[]) this.inMessage.getDataBuilder().getFinalObject();
          received.set(true);
        }

        dataDeserializer.build(this.inMessage, 1);
        message.getByteBuffer().clear();
        recvBuffers.add(message);
      }

      @Override
      public void onSendComplete(int id, int stream, ChannelMessage message) {

      }
    }, recvBuffers);

    while (!received.get()) {
      receiver.progress();
    }

    Assert.assertArrayEquals(data, receivedData[0]);

  }
}
