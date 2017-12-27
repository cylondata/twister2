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
import edu.iu.dsc.tws.comms.mpi.MPIContext;
import edu.iu.dsc.tws.comms.mpi.MPIMessage;
import edu.iu.dsc.tws.comms.mpi.MPIMessageDirection;
import edu.iu.dsc.tws.comms.mpi.MPIMessageReleaseCallback;
import edu.iu.dsc.tws.comms.mpi.MPISendMessage;
import edu.iu.dsc.tws.comms.utils.KryoSerializer;

public final class Test {
  private KryoSerializer serializer;

  private MPIMultiMessageSerializer multiMessageSerializer;

  private MPIMultiMessageDeserializer mpiMultiMessageDeserializer;

  private Queue<MPIBuffer> bufferQueue = new ArrayBlockingQueue<MPIBuffer>(1024);

  public static void main(String[] args) {
    System.out.println("aaaa");

    Test test = new Test();
    test.runTest();
  }

  public Test() {
    serializer = new KryoSerializer();
    serializer.init(null);
    multiMessageSerializer = new MPIMultiMessageSerializer(bufferQueue, serializer);
    mpiMultiMessageDeserializer = new MPIMultiMessageDeserializer(serializer);

    for (int i = 0; i < 100; i++) {
      bufferQueue.offer(new MPIBuffer(1024000));
    }
  }

  public void runTest() {
    IntData data = new IntData();
    MPIMessage message = serializeObject(data, 1);

    deserialize(message);
  }

  private void deserialize(MPIMessage message) {
    List<MPIBuffer> buffers = message.getBuffers();
    for (MPIBuffer mpiBuffer : buffers) {
      mpiBuffer.getByteBuffer().rewind();
    }

    if (message.isComplete()) {
      System.out.printf("Complete message");
    }
    MessageHeader header = mpiMultiMessageDeserializer.buildHeader(message.getBuffers().get(0), 0);
    message.setHeader(header);
    System.out.println(String.format("%d %d %d", header.getLength(),
        header.getSourceId(), header.getEdge()));
    Object d = mpiMultiMessageDeserializer.build(message, 0);
    System.out.println("End");
  }

  private MPIMessage serializeObject(Object object, int source) {
    MultiObject multiObject = new MultiObject(source, object);
    MultiObject multiObject2 = new MultiObject(source, object);
    List<Object> list = new ArrayList<>();
    list.add(multiObject);
    list.add(multiObject2);

    MPIMessage mpiMessage = new MPIMessage(source, MessageType.OBJECT,
        MPIMessageDirection.OUT, new MessageListener());

    int di = -1;
    MPISendMessage sendMessage = new MPISendMessage(source, mpiMessage, 0,
        di, 0, MPIContext.FLAGS_MULTI_MSG, null, null);
    multiMessageSerializer.build(list, sendMessage);

    return mpiMessage;
  }

  private class MessageListener implements MPIMessageReleaseCallback {
    @Override
    public void release(MPIMessage message) {

    }
  }
}
