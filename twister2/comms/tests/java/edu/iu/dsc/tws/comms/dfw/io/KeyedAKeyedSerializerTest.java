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

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.api.MessageTypes;
import edu.iu.dsc.tws.comms.dfw.InMessage;

public class KeyedAKeyedSerializerTest extends BaseSerializeTest {
  @Test
  public void testBuildLargeIntegerMessage() {
    int numBuffers = 10;
    int size = 1000;
    MessageType type = MessageTypes.INTEGER;
    Object data = createKeyedData(800, type);
    InMessage inMessage = keyedSingleValueCase(numBuffers, size, type, type, data);
    Tuple deserializedData = (Tuple) inMessage.getDeserializedData();
    Assert.assertEquals((int) deserializedData.getKey(), (int) ((Tuple) data).getKey());
    Assert.assertArrayEquals((int[]) deserializedData.getValue(),
        (int[]) ((Tuple) data).getValue());
  }

  @Test
  public void testBuildLargeDoubleMessage() {
    int numBuffers = 10;
    int size = 1000;
    MessageType type = MessageTypes.DOUBLE;
    Object data = createKeyedData(800, type);
    InMessage inMessage = keyedSingleValueCase(numBuffers, size, type, type, data);
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
    MessageType type = MessageTypes.LONG;
    Object data = createKeyedData(800, type);
    InMessage inMessage = keyedSingleValueCase(numBuffers, size, type, type, data);
    Tuple deserializedData = (Tuple) inMessage.getDeserializedData();
    Assert.assertEquals((long) deserializedData.getKey(), (long) ((Tuple) data).getKey());
    Assert.assertArrayEquals((long[]) deserializedData.getValue(),
        (long[]) ((Tuple) data).getValue());
  }

  @Test
  public void testBuildLargeShortMessage() {
    int numBuffers = 10;
    int size = 1000;
    MessageType type = MessageTypes.SHORT;
    Object data = createKeyedData(800, type);
    InMessage inMessage = keyedSingleValueCase(numBuffers, size, type, type, data);
    Tuple deserializedData = (Tuple) inMessage.getDeserializedData();
    Assert.assertEquals((short) deserializedData.getKey(), (short) ((Tuple) data).getKey());
    Assert.assertArrayEquals((short[]) deserializedData.getValue(),
        (short[]) ((Tuple) data).getValue());
  }

  @Test
  public void testBuildLargeByteMessage() {
    int numBuffers = 10;
    int size = 1000;
    MessageType type = MessageTypes.BYTE;
    Object data = createKeyedData(800, type);
    InMessage inMessage = keyedSingleValueCase(numBuffers, size, type, type, data);
    Tuple deserializedData = (Tuple) inMessage.getDeserializedData();
    Assert.assertArrayEquals((byte[]) deserializedData.getKey(), (byte[]) ((Tuple) data).getKey());
    Assert.assertArrayEquals((byte[]) deserializedData.getValue(),
        (byte[]) ((Tuple) data).getValue());
  }

  @Test
  public void testBuildIntegerMessage() {
    int numBuffers = 4;
    int size = 1000;
    MessageType type = MessageTypes.INTEGER;
    Object data = createKeyedData(80, type);
    InMessage inMessage = keyedSingleValueCase(numBuffers, size, type, type, data);
    Tuple deserializedData = (Tuple) inMessage.getDeserializedData();
    Assert.assertEquals((int) deserializedData.getKey(), (int) ((Tuple) data).getKey());
    Assert.assertArrayEquals((int[]) deserializedData.getValue(),
        (int[]) ((Tuple) data).getValue());
  }

  @Test
  public void testBuildObjectMessage() {
    int numBuffers = 4;
    int size = 1000;
    MessageType type = MessageTypes.OBJECT;
    Object data = createKeyedData(80, type);
    InMessage inMessage = keyedSingleValueCase(numBuffers, size, type, type, data);
    Tuple deserializedData = (Tuple) inMessage.getDeserializedData();
    Assert.assertArrayEquals((byte[]) deserializedData.getKey(),
        (byte[]) ((Tuple) data).getKey());
    Assert.assertArrayEquals((byte[]) deserializedData.getValue(),
        (byte[]) ((Tuple) data).getValue());
  }

  @SuppressWarnings("Unchecked")
  @Test
  public void testBuildLargeListIntMessage() {
    int numBuffers = 16;
    int size = 1000;
    List<Object> data = new ArrayList<>();
    for (int i = 0; i < 4; i++) {
      Object o = createKeyedData(800, MessageTypes.INTEGER);
      data.add(o);
    }

    InMessage inMessage = listValueCase(numBuffers, size, data, MessageTypes.INTEGER);
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
        Object o = createKeyedData(80, MessageTypes.INTEGER);
        data.add(o);
      }

      InMessage inMessage = listValueCase(numBuffers, size, data, MessageTypes.INTEGER);
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
      Object o = createKeyedData(800, MessageTypes.LONG);
      data.add(o);
    }

    InMessage inMessage = listValueCase(numBuffers, size, data, MessageTypes.LONG);
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
      Object o = createKeyedData(800, MessageTypes.DOUBLE);
      data.add(o);
    }

    InMessage inMessage = listValueCase(numBuffers, size, data, MessageTypes.DOUBLE);
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
      Object o = createKeyedData(800, MessageTypes.SHORT);
      data.add(o);
    }

    InMessage inMessage = listValueCase(numBuffers, size, data, MessageTypes.SHORT);
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
      Object o = createKeyedData(800, MessageTypes.BYTE);
      data.add(o);
    }

    InMessage inMessage = listValueCase(numBuffers, size, data, MessageTypes.BYTE);
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
      Object o = createKeyedData(320, MessageTypes.BYTE, MessageTypes.INTEGER);
      data.add(o);
    }

    InMessage inMessage = keyedListValueCase(numBuffers, size, data, MessageTypes.BYTE,
        MessageTypes.INTEGER);
    List<Object> result = (List<Object>) inMessage.getDeserializedData();
    for (int i = 0; i < result.size(); i++) {
      Tuple deserializedData = (Tuple) result.get(i);
      Tuple d = (Tuple) data.get(i);

      Assert.assertEquals(deserializedData.getKey(), d.getKey());
      Assert.assertArrayEquals((byte[]) deserializedData.getValue(),
          (byte[]) d.getValue());
    }
  }

  private InMessage listValueCase(int numBuffers, int size, List<Object> data,
                                  MessageType type) {
    return keyedListValueCase(numBuffers, size, data, type, type);
  }

  private Object createKeyedData(int size, MessageType dataType) {
    return createKeyedData(size, dataType, dataType);
  }
}
