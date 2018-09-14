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
package edu.iu.dsc.tws.comms.io;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

import edu.iu.dsc.tws.common.examples.utils.IntData;
import edu.iu.dsc.tws.comms.api.MessageHeader;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.dfw.ChannelMessage;
import edu.iu.dsc.tws.comms.dfw.ChannelMessageReleaseCallback;
import edu.iu.dsc.tws.comms.dfw.DataBuffer;
import edu.iu.dsc.tws.comms.dfw.MessageDirection;
import edu.iu.dsc.tws.comms.dfw.OutMessage;
import edu.iu.dsc.tws.comms.dfw.io.KeyedContent;
import edu.iu.dsc.tws.comms.dfw.io.MultiMessageDeserializer;
import edu.iu.dsc.tws.comms.dfw.io.MultiMessageSerializer;
import edu.iu.dsc.tws.comms.utils.KryoSerializer;

@SuppressWarnings({"unchecked", "rawtypes"})
public class Test {
  private KryoSerializer serializer;

  private MultiMessageSerializer multiMessageSerializer;

  private MultiMessageDeserializer multiMessageDeserializer;

  private Queue<DataBuffer> bufferQueue = new ArrayBlockingQueue<DataBuffer>(1024);

  public static void main(String[] args) {
    System.out.println("aaaa");

    Test test = new Test();
    test.runTest2();
  }

  public Test() {
    serializer = new KryoSerializer();
    serializer.init(null);
    multiMessageSerializer = new MultiMessageSerializer(serializer, 0);
    multiMessageSerializer.init(null, bufferQueue, true);
    multiMessageDeserializer = new MultiMessageDeserializer(serializer, 0);
    multiMessageDeserializer.init(null, true);

    for (int i = 0; i < 10; i++) {
      bufferQueue.offer(new DataBuffer(2048));
    }
  }

  @SuppressWarnings("rawtypes")
  public void runTest() {
    IntData data = new IntData();
    List list = new ArrayList<>();
    list.add(data);
    ChannelMessage message = serializeObject(list, 1);

    deserialize(message);
  }

  @SuppressWarnings("rawtypes")
  public void runTest2() {
    IntData data = new IntData(128);
    List list = new ArrayList<>();
    list.add(new KeyedContent(new Short((short) 0), data, MessageType.OBJECT, MessageType.INTEGER));
    data = new IntData(128);
    list.add(new KeyedContent(new Short((short) 1), data, MessageType.OBJECT, MessageType.INTEGER));
    ChannelMessage message = serializeObject(list, 1);
    System.out.println("Serialized first");
    deserialize(message);

//    data = new IntData(128000);
//    list = new ArrayList<>();
//    list.add(new KeyedContent(new Short((short) 2), data));
//    data = new IntData(128000);
//    list.add(new KeyedContent(new Short((short) 3), data));
//    ChannelMessage message2 = serializeObject(list, 1);
////    System.out.println("Serialized second");
////
//    list = new ArrayList<>();
//    list.add(message);
//    list.add(message2);
////    data = new IntData(128000);
////    list.add(new MultiObject(1, data));
//
//    ChannelMessage second = serializeObject(list, 1);
//
//    deserialize(second);
  }

  private void deserialize(ChannelMessage message) {
    List<DataBuffer> buffers = message.getBuffers();
    for (DataBuffer dataBuffer : buffers) {
      dataBuffer.getByteBuffer().flip();
      dataBuffer.getByteBuffer().rewind();
    }

    if (message.isComplete()) {
      System.out.printf("Complete message");
    }
    message.setKeyType(MessageType.SHORT);
    MessageHeader header = multiMessageDeserializer.buildHeader(message.getBuffers().get(0), 0);
    message.setHeader(header);
    System.out.println(String.format("%d %d %d", header.getLength(),
        header.getSourceId(), header.getEdge()));
    Object d = multiMessageDeserializer.build(message, 0);
    List list = (List) d;
    for (Object o : list) {
      if (o instanceof KeyedContent) {
        System.out.println(((KeyedContent) o).getKey());
        if (((KeyedContent) o).getValue() instanceof IntData) {
          System.out.println("Length: "
              + ((IntData) ((KeyedContent) o).getValue()).getData().length);
        }
      }
    }
    System.out.println("End");
  }

  private ChannelMessage serializeObject(List object, int source) {
    ChannelMessage channelMessage = new ChannelMessage(source, MessageType.OBJECT,
        MessageDirection.OUT, new MessageListener());
    channelMessage.setKeyType(MessageType.INTEGER);

    int di = -1;
    OutMessage sendMessage = new OutMessage(source, channelMessage, 0,
        di, 0, 0, null, null);
    multiMessageSerializer.build(object, sendMessage);

    return channelMessage;
  }

  private class MessageListener implements ChannelMessageReleaseCallback {
    @Override
    public void release(ChannelMessage message) {

    }
  }
}
