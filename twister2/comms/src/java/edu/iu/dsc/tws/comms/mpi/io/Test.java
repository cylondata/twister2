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
package edu.iu.dsc.tws.comms.mpi.io;

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

import edu.iu.dsc.tws.comms.api.MessageHeader;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.mpi.MPIBuffer;
import edu.iu.dsc.tws.comms.mpi.MPIMessage;
import edu.iu.dsc.tws.comms.mpi.MPIMessageDirection;
import edu.iu.dsc.tws.comms.mpi.MPIMessageReleaseCallback;
import edu.iu.dsc.tws.comms.mpi.MPISendMessage;
import edu.iu.dsc.tws.comms.utils.KryoSerializer;

@SuppressWarnings({"rawtypes", "unchecked"})
public final class Test {
  private KryoSerializer serializer;

  private MPIMultiMessageSerializer multiMessageSerializer;

  private MPIMultiMessageDeserializer mpiMultiMessageDeserializer;

  private Queue<MPIBuffer> bufferQueue = new ArrayBlockingQueue<MPIBuffer>(1024);

  public static void main(String[] args) {
    System.out.println("aaaa");

    Test test = new Test();
    test.runTest2();
  }

  public Test() {
    serializer = new KryoSerializer();
    serializer.init(null);
    multiMessageSerializer = new MPIMultiMessageSerializer(serializer, 0);
    multiMessageSerializer.init(null, bufferQueue, true);
    mpiMultiMessageDeserializer = new MPIMultiMessageDeserializer(serializer, 0);
    mpiMultiMessageDeserializer.init(null, true);

    for (int i = 0; i < 10; i++) {
      bufferQueue.offer(new MPIBuffer(2048));
    }
  }

  @SuppressWarnings("rawtypes")
  public void runTest() {
    IntData data = new IntData();
    List list = new ArrayList<>();
    list.add(data);
    MPIMessage message = serializeObject(list, 1);

    deserialize(message);
  }

  @SuppressWarnings("rawtypes")
  public void runTest2() {
    IntData data = new IntData(128);
    List list = new ArrayList<>();
    list.add(new KeyedContent(new Short((short) 0), data));
    data = new IntData(128);
    list.add(new KeyedContent(new Short((short) 1), data));
    MPIMessage message = serializeObject(list, 1);
    System.out.println("Serialized first");
    deserialize(message);

//    data = new IntData(128000);
//    list = new ArrayList<>();
//    list.add(new KeyedContent(new Short((short) 2), data));
//    data = new IntData(128000);
//    list.add(new KeyedContent(new Short((short) 3), data));
//    MPIMessage message2 = serializeObject(list, 1);
////    System.out.println("Serialized second");
////
//    list = new ArrayList<>();
//    list.add(message);
//    list.add(message2);
////    data = new IntData(128000);
////    list.add(new MultiObject(1, data));
//
//    MPIMessage second = serializeObject(list, 1);
//
//    deserialize(second);
  }

  private void deserialize(MPIMessage message) {
    List<MPIBuffer> buffers = message.getBuffers();
    for (MPIBuffer mpiBuffer : buffers) {
      mpiBuffer.getByteBuffer().flip();
      mpiBuffer.getByteBuffer().rewind();
    }

    if (message.isComplete()) {
      System.out.printf("Complete message");
    }
    message.setKeyType(MessageType.SHORT);
    MessageHeader header = mpiMultiMessageDeserializer.buildHeader(message.getBuffers().get(0), 0);
    message.setHeader(header);
    System.out.println(String.format("%d %d %d", header.getLength(),
        header.getSourceId(), header.getEdge()));
    Object d = mpiMultiMessageDeserializer.build(message, 0);
    List list = (List) d;
    for (Object o : list) {
      if (o instanceof KeyedContent) {
        System.out.println(((KeyedContent) o).getSource());
        if (((KeyedContent) o).getObject() instanceof IntData) {
          System.out.println("Length: "
              + ((IntData) ((KeyedContent) o).getObject()).getData().length);
        }
      }
    }
    System.out.println("End");
  }

  private MPIMessage serializeObject(List object, int source) {
    MPIMessage mpiMessage = new MPIMessage(source, MessageType.OBJECT,
        MPIMessageDirection.OUT, new MessageListener());
    mpiMessage.setKeyType(MessageType.INTEGER);

    int di = -1;
    MPISendMessage sendMessage = new MPISendMessage(source, mpiMessage, 0,
        di, 0, 0, null, null);
    multiMessageSerializer.build(object, sendMessage);

    return mpiMessage;
  }

  private class MessageListener implements MPIMessageReleaseCallback {
    @Override
    public void release(MPIMessage message) {

    }
  }
}
