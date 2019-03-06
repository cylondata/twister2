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

public class KeyedUnifiedSerializerTest {
  @Test
  public void testBuildLargeIntegerMessage() {
    int numBuffers = 10;
    int size = 1000;
    MessageType type = MessageType.INTEGER;
    Object data = createData(800, type);
    InMessage inMessage = singleValueCase(numBuffers, size, type, data);
    Tuple deserializedData = (Tuple) inMessage.getDeserializedData();
    Assert.assertEquals((int) deserializedData.getKey(), (int) ((Tuple) data).getKey());
    Assert.assertArrayEquals((int[]) deserializedData.getValue(),
        (int[]) ((Tuple) data).getValue());
  }

  @Test
  public void testBuildLargeDoubleMessage() {
    int numBuffers = 10;
    int size = 1000;
    MessageType type = MessageType.DOUBLE;
    Object data = createData(800, type);
    InMessage inMessage = singleValueCase(numBuffers, size, type, data);
    Tuple deserializedData = (Tuple) inMessage.getDeserializedData();
    Assert.assertEquals((double) deserializedData.getKey(), (double) ((Tuple) data).getKey(),
        0.1);
    Assert.assertArrayEquals((double[]) deserializedData.getValue(),
        (double[]) ((Tuple) data).getValue(), 0.01);
  }

  @Test
  public void testBuildLargeLongMessage() {
    int numBuffers = 10;
    int size = 1000;
    MessageType type = MessageType.LONG;
    Object data = createData(800, type);
    InMessage inMessage = singleValueCase(numBuffers, size, type, data);
    Tuple deserializedData = (Tuple) inMessage.getDeserializedData();
    Assert.assertEquals((long) deserializedData.getKey(), (long) ((Tuple) data).getKey());
    Assert.assertArrayEquals((long[]) deserializedData.getValue(),
        (long[]) ((Tuple) data).getValue());
  }

  @Test
  public void testBuildLargeShortMessage() {
    int numBuffers = 10;
    int size = 1000;
    MessageType type = MessageType.SHORT;
    Object data = createData(800, type);
    InMessage inMessage = singleValueCase(numBuffers, size, type, data);
    Tuple deserializedData = (Tuple) inMessage.getDeserializedData();
    Assert.assertEquals((short) deserializedData.getKey(), (short) ((Tuple) data).getKey());
    Assert.assertArrayEquals((short[]) deserializedData.getValue(),
        (short[]) ((Tuple) data).getValue());
  }

  @Test
  public void testBuildLargeByteMessage() {
    int numBuffers = 10;
    int size = 1000;
    MessageType type = MessageType.BYTE;
    Object data = createData(800, type);
    InMessage inMessage = singleValueCase(numBuffers, size, type, data);
    Tuple deserializedData = (Tuple) inMessage.getDeserializedData();
    Assert.assertArrayEquals((byte[]) deserializedData.getKey(), (byte[]) ((Tuple) data).getKey());
    Assert.assertArrayEquals((byte[]) deserializedData.getValue(),
        (byte[]) ((Tuple) data).getValue());
  }

  @Test
  public void testBuildIntegerMessage() {
    int numBuffers = 4;
    int size = 1000;
    MessageType type = MessageType.INTEGER;
    Object data = createData(80, type);
    InMessage inMessage = singleValueCase(numBuffers, size, type, data);
    Tuple deserializedData = (Tuple) inMessage.getDeserializedData();
    Assert.assertEquals((int) deserializedData.getKey(), (int) ((Tuple) data).getKey());
    Assert.assertArrayEquals((int[]) deserializedData.getValue(),
        (int[]) ((Tuple) data).getValue());
  }

  @Test
  public void testBuildObjectMessage() {
    int numBuffers = 4;
    int size = 1000;
    MessageType type = MessageType.OBJECT;
    Object data = createData(80, type);
    InMessage inMessage = singleValueCase(numBuffers, size, type, data);
    Tuple deserializedData = (Tuple) inMessage.getDeserializedData();
    Assert.assertArrayEquals((byte[]) deserializedData.getKey(),
        (byte[]) ((Tuple) data).getKey());
    Assert.assertArrayEquals((byte[]) deserializedData.getValue(),
        (byte[]) ((Tuple) data).getValue());
  }

  private InMessage singleValueCase(int numBuffers, int size, MessageType type, Object data) {
    BlockingQueue<DataBuffer> bufferQueue = createDataQueue(numBuffers, size);

    OutMessage outMessage = new OutMessage(0, 1, -1, 10, 0, null,
        null, type, null, null);

    UnifiedKeySerializer serializer = new UnifiedKeySerializer(
        new KryoSerializer(), 0, type, type);
    serializer.init(Config.newBuilder().build(), bufferQueue, true);

    List<ChannelMessage> messages = new ArrayList<>();

    while (outMessage.getSendState() != OutMessage.SendState.SERIALIZED) {
      ChannelMessage ch = (ChannelMessage) serializer.build(data, outMessage);
      messages.add(ch);
    }

    UnifiedKeyDeSerializer deserializer = new UnifiedKeyDeSerializer(
        new KryoSerializer(), 0, type, type);
    deserializer.init(Config.newBuilder().build(), true);

    MessageHeader header = deserializer.buildHeader(
        messages.get(0).getBuffers().get(0), 1);
    InMessage inMessage = new InMessage(0, type,
        null, header);
    inMessage.setKeyType(type);
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
      Tuple exp = (Tuple) result.get(i);
      Tuple d = (Tuple) data.get(i);

      Assert.assertEquals((int) exp.getKey(), (int) ((Tuple) d).getKey());
      Assert.assertArrayEquals((int[]) exp.getValue(),
          (int[]) ((Tuple) d).getValue());
    }
  }

  @SuppressWarnings("Unchecked")
  @Test
  public void testBuildListIntMessage() {
    int numBuffers = 128;
    int size = 1000;

    for (int j = 1; j < 128; j++) {
      List<Object> data = new ArrayList<>();
      for (int i = 0; i < j; i++) {
        Object o = createData(80, MessageType.INTEGER);
        data.add(o);
      }

      InMessage inMessage = listValueCase(numBuffers, size, data, MessageType.INTEGER);
      try {
        List<Object> result = (List<Object>) inMessage.getDeserializedData();
        for (int i = 0; i < result.size(); i++) {
          Tuple exp = (Tuple) result.get(i);
          Tuple d = (Tuple) data.get(i);

          Assert.assertEquals((int) exp.getKey(), (int) ((Tuple) d).getKey());
          Assert.assertArrayEquals((int[]) exp.getValue(),
              (int[]) ((Tuple) d).getValue());
        }
      } catch (NullPointerException e) {
        Assert.fail("j = " + j);
      }
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
      Tuple deserializedData = (Tuple) result.get(i);
      Tuple d = (Tuple) data.get(i);

      Assert.assertEquals((long) deserializedData.getKey(), (long) ((Tuple) d).getKey(),
          0.1);
      Assert.assertArrayEquals((long[]) deserializedData.getValue(),
          (long[]) ((Tuple) d).getValue());
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
      Tuple deserializedData = (Tuple) result.get(i);
      Tuple d = (Tuple) data.get(i);

      Assert.assertEquals((double) deserializedData.getKey(), (double) ((Tuple) d).getKey(),
          0.1);
      Assert.assertArrayEquals((double[]) deserializedData.getValue(),
          (double[]) ((Tuple) d).getValue(), 0.01);
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
      Tuple deserializedData = (Tuple) result.get(i);
      Tuple d = (Tuple) data.get(i);

      Assert.assertEquals((short) deserializedData.getKey(), (short) ((Tuple) d).getKey());
      Assert.assertArrayEquals((short[]) deserializedData.getValue(),
          (short[]) ((Tuple) d).getValue());
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
      Tuple deserializedData = (Tuple) result.get(i);
      Tuple d = (Tuple) data.get(i);

      Assert.assertArrayEquals((byte[]) deserializedData.getKey(), (byte[]) ((Tuple) d).getKey());
      Assert.assertArrayEquals((byte[]) deserializedData.getValue(),
          (byte[]) ((Tuple) d).getValue());
    }
  }

  @SuppressWarnings("Unchecked")
  @Test
  public void testBuildLargeListIntegerByteMessage() {
    int numBuffers = 128;
    int size = 1000;
    List<Object> data = new ArrayList<>();
    for (int i = 0; i < 128; i++) {
      Object o = createData(320, MessageType.BYTE, MessageType.INTEGER);
      data.add(o);
    }

    InMessage inMessage = listValueCase(numBuffers, size, data, MessageType.BYTE,
        MessageType.INTEGER);
    List<Object> result = (List<Object>) inMessage.getDeserializedData();
    for (int i = 0; i < result.size(); i++) {
      Tuple deserializedData = (Tuple) result.get(i);
      Tuple d = (Tuple) data.get(i);

      Assert.assertEquals(deserializedData.getKey(), d.getKey());
      Assert.assertArrayEquals((byte[]) deserializedData.getValue(),
          (byte[]) ((Tuple) d).getValue());
    }
  }

  private InMessage listValueCase(int numBuffers, int size, List<Object> data,
                                  MessageType type) {
    return listValueCase(numBuffers, size, data, type, type);
  }

  private InMessage listValueCase(int numBuffers, int size, List<Object> data,
                                  MessageType type, MessageType keyType) {
    BlockingQueue<DataBuffer> bufferQueue = createDataQueue(numBuffers, size);
    OutMessage outMessage = new OutMessage(0, 1, -1, 10, 0, null,
        null, type, null, null);

    UnifiedKeySerializer serializer = new UnifiedKeySerializer(
        new KryoSerializer(), 0, keyType, type);
    serializer.init(Config.newBuilder().build(), bufferQueue, true);

    List<ChannelMessage> messages = new ArrayList<>();


    while (outMessage.getSendState() != OutMessage.SendState.SERIALIZED) {
      ChannelMessage ch = (ChannelMessage) serializer.build(data, outMessage);
      messages.add(ch);
    }

    UnifiedKeyDeSerializer deserializer = new UnifiedKeyDeSerializer(new KryoSerializer(), 0,
        keyType, type);
    deserializer.init(Config.newBuilder().build(), true);

    MessageHeader header = deserializer.buildHeader(
        messages.get(0).getBuffers().get(0), 1);
    InMessage inMessage = new InMessage(0, type,
        null, header);
    inMessage.setKeyType(keyType);
    for (ChannelMessage channelMessage : messages) {
      for (DataBuffer dataBuffer : channelMessage.getBuffers()) {
        inMessage.addBufferAndCalculate(dataBuffer);
      }
    }
    deserializer.build(inMessage, 1);
    return inMessage;
  }

  private Object createData(int size, MessageType dataType) {
    return createData(size, dataType, dataType);
  }

  private Object createData(int size, MessageType dataType, MessageType keyType) {
    Object data = createDataObject(size, dataType);
    Object key = createKeyObject(keyType);
    return new Tuple(key, data, keyType, dataType);
  }

  private Object createDataObject(int size, MessageType dataType) {
    if (dataType == MessageType.INTEGER) {
      int[] vals = new int[size];
      for (int i = 0; i < vals.length; i++) {
        vals[i] = i;
      }
      return vals;
    } else if (dataType == MessageType.LONG) {
      long[] vals = new long[size];
      for (int i = 0; i < vals.length; i++) {
        vals[i] = i;
      }
      return vals;
    } else if (dataType == MessageType.DOUBLE) {
      double[] vals = new double[size];
      for (int i = 0; i < vals.length; i++) {
        vals[i] = i;
      }
      return vals;
    } else if (dataType == MessageType.SHORT) {
      short[] vals = new short[size];
      for (int i = 0; i < vals.length; i++) {
        vals[i] = (short) i;
      }
      return vals;
    } else if (dataType == MessageType.BYTE) {
      byte[] vals = new byte[size];
      for (int i = 0; i < vals.length; i++) {
        vals[i] = (byte) i;
      }
      return vals;
    } else if (dataType == MessageType.OBJECT) {
      byte[] vals = new byte[size];
      for (int i = 0; i < vals.length; i++) {
        vals[i] = (byte) i;
      }
      return vals;
    } else {
      return null;
    }
  }

  private Object createKeyObject(MessageType dataType) {
    if (dataType == MessageType.INTEGER) {
      return 1;
    } else if (dataType == MessageType.LONG) {
      return 1L;
    } else if (dataType == MessageType.DOUBLE) {
      return 1.0;
    } else if (dataType == MessageType.SHORT) {
      return (short) 1;
    } else if (dataType == MessageType.BYTE) {
      byte[] vals = new byte[8];
      for (int i = 0; i < vals.length; i++) {
        vals[i] = (byte) i;
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
