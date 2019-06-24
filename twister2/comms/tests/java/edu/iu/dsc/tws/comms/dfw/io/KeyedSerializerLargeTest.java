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
import java.util.concurrent.BlockingQueue;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.comms.messaging.MessageHeader;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageType;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.comms.messaging.ChannelMessage;
import edu.iu.dsc.tws.api.comms.packing.DataBuffer;
import edu.iu.dsc.tws.comms.dfw.InMessage;
import edu.iu.dsc.tws.comms.dfw.OutMessage;

public class KeyedSerializerLargeTest extends BaseSerializeTest {
  private KeyedDataSerializer serializer;

  private BlockingQueue<DataBuffer> bufferQueue;

  private KeyedDataDeSerializer deserializer;

  @Before
  public void setUp() throws Exception {
    bufferQueue = createDataQueue(32, 1024000);
    serializer = new KeyedDataSerializer();
    serializer.init(Config.newBuilder().build(), bufferQueue, true);

    deserializer = new KeyedDataDeSerializer();
    deserializer.init(Config.newBuilder().build(), true);
  }

  @SuppressWarnings("Unchecked")
  @Test
  public void testBuildLargeListByteMessage() {
    int numBuffers = 32;
    int size = 1024000;

    for (int numObjects = 1; numObjects < 5000; numObjects++) {
      System.out.println("Starting test : " + numObjects);
      List<Object> data = new AggregatedObjects<>();
      for (int i = 0; i < numObjects; i++) {
        Object o = createKeyedData(100, MessageTypes.INTEGER_ARRAY, 1, MessageTypes.INTEGER);
        data.add(o);
      }

      InMessage inMessage = keyedListValueCase(numBuffers, size, data,
          MessageTypes.INTEGER_ARRAY, MessageTypes.INTEGER);

      List<Object> result = (List<Object>) inMessage.getDeserializedData();

      Assert.assertEquals(numObjects, result.size());

      for (int i = 0; i < result.size(); i++) {
        Tuple deserializedData = (Tuple) result.get(i);
        Tuple d = (Tuple) data.get(i);

        Assert.assertEquals(deserializedData.getKey(), d.getKey());
        Assert.assertArrayEquals((int[]) deserializedData.getValue(),
            (int[]) ((Tuple) d).getValue());
      }
    }
  }

  public InMessage keyedListValueCase(int numBuffers, int size, List<Object> data,
                                      MessageType type, MessageType keyType) {
    OutMessage outMessage = new OutMessage(0, 1, -1, 10, 0, null,
        null, type, keyType, null, data);

    List<ChannelMessage> messages = new ArrayList<>();

    int count = 0;
    while (outMessage.getSendState() != OutMessage.SendState.SERIALIZED) {
      ChannelMessage ch = (ChannelMessage) serializer.build(data, outMessage);
      messages.add(ch);
      System.out.println("Adding count " + count++);
    }

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
    for (ChannelMessage d : inMessage.getBuiltMessages()) {
      for (DataBuffer buffer : d.getNormalBuffers()) {
        buffer.getByteBuffer().clear();
        bufferQueue.offer(buffer);
      }
    }
    return inMessage;
  }
}
