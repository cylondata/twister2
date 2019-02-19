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
  public void testBuildLargeIntegerMessage() {
    int numBuffers = 10;
    int size = 1000;
    MessageType type = MessageType.INTEGER;
    Object data = createData(800, type);
    InMessage inMessage = singleValueCase(numBuffers, size, type, data);
    Assert.assertArrayEquals((int[]) inMessage.getDeserializedData(), (int[]) data);
  }

  @Test
  public void testBuildLargeDoubleMessage() {
    int numBuffers = 10;
    int size = 1000;
    MessageType type = MessageType.DOUBLE;
    Object data = createData(800, type);
    InMessage inMessage = singleValueCase(numBuffers, size, type, data);
    Assert.assertArrayEquals((double[]) inMessage.getDeserializedData(), (double[]) data, .01);
  }

  @Test
  public void testBuildLargeLongMessage() {
    int numBuffers = 10;
    int size = 1000;
    MessageType type = MessageType.LONG;
    Object data = createData(800, type);
    InMessage inMessage = singleValueCase(numBuffers, size, type, data);
    Assert.assertArrayEquals((long[]) inMessage.getDeserializedData(), (long[]) data);
  }

  @Test
  public void testBuildLargeShortMessage() {
    int numBuffers = 10;
    int size = 1000;
    MessageType type = MessageType.SHORT;
    Object data = createData(800, type);
    InMessage inMessage = singleValueCase(numBuffers, size, type, data);
    Assert.assertArrayEquals((short[]) inMessage.getDeserializedData(), (short[]) data);
  }

  @Test
  public void testBuildLargeByteMessage() {
    int numBuffers = 10;
    int size = 1000;
    MessageType type = MessageType.BYTE;
    Object data = createData(800, type);
    InMessage inMessage = singleValueCase(numBuffers, size, type, data);
    Assert.assertArrayEquals((byte[]) inMessage.getDeserializedData(), (byte[]) data);
  }

  @Test
  public void testBuildLargeObjectMessage() {
    int numBuffers = 20;
    int size = 1000;
    MessageType type = MessageType.OBJECT;
    Object data = createData(800, type);
    InMessage inMessage = singleValueCase(numBuffers, size, type, data);
    Assert.assertArrayEquals((int[]) inMessage.getDeserializedData(), (int[]) data);
  }

  private InMessage singleValueCase(int numBuffers, int size, MessageType type, Object data) {
    BlockingQueue<DataBuffer> bufferQueue = createDataQueue(numBuffers, size);

    OutMessage outMessage = new OutMessage(0, 1, -1, 10, 0, null,
        null, type, null, null);

    UnifiedSerializer serializer = new UnifiedSerializer(new KryoSerializer(), 0, type);
    serializer.init(Config.newBuilder().build(), bufferQueue, false);

    List<ChannelMessage> messages = new ArrayList<>();

    while (outMessage.getSendState() != OutMessage.SendState.SERIALIZED) {
      ChannelMessage ch = (ChannelMessage) serializer.build(data, outMessage);
      messages.add(ch);
    }

    UnifiedDeserializer deserializer = new UnifiedDeserializer(new KryoSerializer(), 0, type);
    deserializer.init(Config.newBuilder().build(), false);

    MessageHeader header = deserializer.buildHeader(
        messages.get(0).getBuffers().get(0), 1);
    InMessage inMessage = new InMessage(0, type,
        null, header);
    for (ChannelMessage channelMessage : messages) {
      for (DataBuffer dataBuffer : channelMessage.getBuffers()) {
        inMessage.addBufferAndCalculate(dataBuffer);
      }
    }
    deserializer.build(inMessage, 1);
    return inMessage;
  }

  @SuppressWarnings("Unchecked")
  @Test
  public void testBuildLargeListIntMessage() {
    int numBuffers = 16;
    int size = 1000;
    List<Object> data = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      Object o = createData(800, MessageType.INTEGER);
      data.add(o);
    }

    InMessage inMessage = listValueCase(numBuffers, size, data, MessageType.INTEGER);
    List<Object> result = (List<Object>) inMessage.getDeserializedData();
    for (int i = 0; i < result.size(); i++) {
      Object exp = result.get(i);
      Object d = data.get(i);

      Assert.assertArrayEquals((int[]) exp, (int[]) d);
    }
  }

  @SuppressWarnings("Unchecked")
  @Test
  public void testBuildLargeListLongMessage() {
    int numBuffers = 32;
    int size = 1000;
    List<Object> data = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      Object o = createData(800, MessageType.LONG);
      data.add(o);
    }

    InMessage inMessage = listValueCase(numBuffers, size, data, MessageType.LONG);
    List<Object> result = (List<Object>) inMessage.getDeserializedData();
    for (int i = 0; i < result.size(); i++) {
      Object exp = result.get(i);
      Object d = data.get(i);

      Assert.assertArrayEquals((long[]) exp, (long[]) d);
    }
  }

  @SuppressWarnings("Unchecked")
  @Test
  public void testBuildLargeListDoubleMessage() {
    int numBuffers = 32;
    int size = 1000;
    List<Object> data = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      Object o = createData(800, MessageType.DOUBLE);
      data.add(o);
    }

    InMessage inMessage = listValueCase(numBuffers, size, data, MessageType.DOUBLE);
    List<Object> result = (List<Object>) inMessage.getDeserializedData();
    for (int i = 0; i < result.size(); i++) {
      Object exp = result.get(i);
      Object d = data.get(i);

      Assert.assertArrayEquals((double[]) exp, (double[]) d, 0.01);
    }
  }

  @SuppressWarnings("Unchecked")
  @Test
  public void testBuildLargeListShortMessage() {
    int numBuffers = 32;
    int size = 1000;
    List<Object> data = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      Object o = createData(800, MessageType.SHORT);
      data.add(o);
    }

    InMessage inMessage = listValueCase(numBuffers, size, data, MessageType.SHORT);
    List<Object> result = (List<Object>) inMessage.getDeserializedData();
    for (int i = 0; i < result.size(); i++) {
      Object exp = result.get(i);
      Object d = data.get(i);

      Assert.assertArrayEquals((short[]) exp, (short[]) d);
    }
  }

  @SuppressWarnings("Unchecked")
  @Test
  public void testBuildLargeListByteMessage() {
    int numBuffers = 32;
    int size = 1000;
    List<Object> data = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      Object o = createData(800, MessageType.BYTE);
      data.add(o);
    }

    InMessage inMessage = listValueCase(numBuffers, size, data, MessageType.BYTE);
    List<Object> result = (List<Object>) inMessage.getDeserializedData();
    for (int i = 0; i < result.size(); i++) {
      Object exp = result.get(i);
      Object d = data.get(i);

      Assert.assertArrayEquals((byte[]) exp, (byte[]) d);
    }
  }

  private InMessage listValueCase(int numBuffers, int size, List<Object> data, MessageType type) {
    BlockingQueue<DataBuffer> bufferQueue = createDataQueue(numBuffers, size);
    OutMessage outMessage = new OutMessage(0, 1, -1, 10, 0, null,
        null, type, null, null);

    UnifiedSerializer serializer = new UnifiedSerializer(new KryoSerializer(), 0, type);
    serializer.init(Config.newBuilder().build(), bufferQueue, false);

    List<ChannelMessage> messages = new ArrayList<>();


    while (outMessage.getSendState() != OutMessage.SendState.SERIALIZED) {
      ChannelMessage ch = (ChannelMessage) serializer.build(data, outMessage);
      messages.add(ch);
    }

    UnifiedDeserializer deserializer = new UnifiedDeserializer(new KryoSerializer(), 0, type);
    deserializer.init(Config.newBuilder().build(), false);

    MessageHeader header = deserializer.buildHeader(
        messages.get(0).getBuffers().get(0), 1);
    InMessage inMessage = new InMessage(0, type,
        null, header);
    for (ChannelMessage channelMessage : messages) {
      for (DataBuffer dataBuffer : channelMessage.getBuffers()) {
        inMessage.addBufferAndCalculate(dataBuffer);
      }
    }
    deserializer.build(inMessage, 1);
    return inMessage;
  }

  public static Object createData(int size, MessageType type) {
    if (type == MessageType.INTEGER) {
      int[] vals = new int[size];
      for (int i = 0; i < vals.length; i++) {
        vals[i] = i;
      }
      return vals;
    } else if (type == MessageType.LONG) {
      long[] vals = new long[size];
      for (int i = 0; i < vals.length; i++) {
        vals[i] = i;
      }
      return vals;
    } else if (type == MessageType.DOUBLE) {
      double[] vals = new double[size];
      for (int i = 0; i < vals.length; i++) {
        vals[i] = i;
      }
      return vals;
    } else if (type == MessageType.SHORT) {
      short[] vals = new short[size];
      for (int i = 0; i < vals.length; i++) {
        vals[i] = (short) i;
      }
      return vals;
    } else if (type == MessageType.BYTE) {
      byte[] vals = new byte[size];
      for (int i = 0; i < vals.length; i++) {
        vals[i] = (byte) i;
      }
      return vals;
    } else if (type == MessageType.OBJECT) {
      int[] vals = new int[size];
      for (int i = 0; i < vals.length; i++) {
        vals[i] = i;
      }
      return vals;
    } else {
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
