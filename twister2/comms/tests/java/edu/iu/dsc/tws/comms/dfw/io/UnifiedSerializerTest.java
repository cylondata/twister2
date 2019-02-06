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
package edu.iu.dsc.tws.comms.dfw.io;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.junit.Assert;
import org.junit.Test;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.MessageHeader;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.dfw.ChannelMessage;
import edu.iu.dsc.tws.comms.dfw.DataBuffer;
import edu.iu.dsc.tws.comms.dfw.InMessage;
import edu.iu.dsc.tws.comms.dfw.OutMessage;
import edu.iu.dsc.tws.comms.utils.KryoSerializer;

public class UnifiedSerializerTest {
  @Test
  public void testBuildLargeMessage() {
    int numBuffers = 10;
    int size = 1000;
    BlockingQueue<DataBuffer> bufferQueue = createDataQueue(numBuffers, size);

    OutMessage outMessage = new OutMessage(0, 1, -1, 10, 0, null,
        null, MessageType.INTEGER, null, null);

    UnifiedSerializer serializer = new UnifiedSerializer(new KryoSerializer(), 0);
    serializer.init(Config.newBuilder().build(), bufferQueue, false);

    List<ChannelMessage> messages = new ArrayList<>();

    Object data = createData(800, MessageType.INTEGER);
    while (outMessage.getSendState() != OutMessage.SendState.SERIALIZED) {
      ChannelMessage ch = (ChannelMessage) serializer.build(data, outMessage);
      messages.add(ch);
    }

    UnifiedDeserializer deserializer = new UnifiedDeserializer(new KryoSerializer(), 0);
    deserializer.init(Config.newBuilder().build(), false);

    MessageHeader header = deserializer.buildHeader(
        outMessage.getChannelMessages().peek().getBuffers().get(0), 1);
    InMessage inMessage = new InMessage(0, MessageType.INTEGER,
        null, header);
    for (ChannelMessage channelMessage : messages) {
      inMessage.addBuiltMessage(channelMessage);
    }
    deserializer.build(inMessage, 1);

    Assert.assertEquals(inMessage.getReceivedState(), InMessage.ReceivedState.BUILT);
  }

  private Object createData(int size, MessageType type) {
    switch (type) {
      case INTEGER:
        return new int[size];
      default:
        return null;
    }
  }

  private BlockingQueue<DataBuffer> createDataQueue(int numBuffers, int size) {
    BlockingQueue<DataBuffer> bufferQueue = new LinkedBlockingQueue<DataBuffer>();
    for (int i = 0; i < numBuffers; i++) {
      bufferQueue.offer(new DataBuffer(ByteBuffer.allocate(size)));
    }
    return bufferQueue;
  }
}
