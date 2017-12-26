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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.MessageDeSerializer;
import edu.iu.dsc.tws.comms.api.MessageHeader;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.mpi.MPIBuffer;
import edu.iu.dsc.tws.comms.mpi.MPIMessage;
import edu.iu.dsc.tws.comms.utils.KryoSerializer;

public class MPIMultiMessageDeserializer implements MessageDeSerializer {
  private static final Logger LOG = Logger.getLogger(MPIMultiMessageDeserializer.class.getName());

  private KryoSerializer serializer;

  public MPIMultiMessageDeserializer(KryoSerializer kryoSerializer) {
    this.serializer = kryoSerializer;
  }

  @Override
  public void init(Config cfg) {

  }

  @Override
  public Object build(Object partialObject, int edge) {
    MPIMessage currentMessage = (MPIMessage) partialObject;
    int readLength = 0;
    int bufferIndex = 0;
    List<MPIBuffer> buffers = currentMessage.getBuffers();
    while (readLength < currentMessage.getHeader().getLength()) {
      List<MPIBuffer> messageBuffers = new ArrayList<>();
      // now read the header
      MPIBuffer mpiBuffer = buffers.get(bufferIndex);
      ByteBuffer byteBuffer = mpiBuffer.getByteBuffer();
      int length = byteBuffer.getInt();
      int source = byteBuffer.getInt();


    }
    return buildMessage(currentMessage, 0);
  }

  @Override
  public MessageHeader buildHeader(MPIBuffer buffer, int edge) {
    int sourceId = buffer.getByteBuffer().getInt();
    int flags = buffer.getByteBuffer().getInt();
    int destId = buffer.getByteBuffer().getInt();
    int length = buffer.getByteBuffer().getInt();

    MessageHeader.Builder headerBuilder = MessageHeader.newBuilder(
        sourceId, edge, length);
    headerBuilder.flags(flags);
    headerBuilder.destination(destId);
    return headerBuilder.build();
  }

  private Object buildMessage(MPIMessage message, int a) {
    MessageType type = message.getType();

    switch (type) {
      case INTEGER:
        break;
      case LONG:
        break;
      case DOUBLE:
        break;
      case OBJECT:
        return buildObject(message);
      case BYTE:
        break;
      case STRING:
        break;
      case BUFFER:
        break;
      default:
        break;
    }
    return null;
  }

  private Object buildObject(MPIMessage message) {
    MPIByteArrayInputStream input = null;
    try {
      //todo: headersize
      input = new MPIByteArrayInputStream(message.getBuffers(), message.getHeaderSize(),
          message.getHeader().getLength());
      return serializer.deserialize(input);
    } finally {
      if (input != null) {
        try {
          input.close();
        } catch (IOException ignore) {
        }
      }
    }
  }
}
