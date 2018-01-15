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
import edu.iu.dsc.tws.comms.api.MessageHeader;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.mpi.MPIBuffer;
import edu.iu.dsc.tws.comms.mpi.MPIMessage;
import edu.iu.dsc.tws.comms.utils.KryoSerializer;

public class MPIMultiMessageDeserializer implements MessageDeSerializer {
  private static final Logger LOG = Logger.getLogger(MPIMultiMessageDeserializer.class.getName());

  private KryoSerializer serializer;

  private int executor;

  public MPIMultiMessageDeserializer(KryoSerializer kryoSerializer, int exec) {
    this.serializer = kryoSerializer;
    this.executor = exec;
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
    List<KeyedContent> returnList = new ArrayList<>();
    MessageHeader header = currentMessage.getHeader();

    if (header == null) {
      throw new RuntimeException("Header must be built before the message");
    }

    while (readLength < header.getLength()) {
      List<MPIBuffer> messageBuffers = new ArrayList<>();
      // now read the header
      MPIBuffer mpiBuffer = buffers.get(bufferIndex);
      ByteBuffer byteBuffer = mpiBuffer.getByteBuffer();
      int length = byteBuffer.getInt();
      int keyLength = byteBuffer.getInt();
      int source = byteBuffer.getShort();

      int tempLength = 0;
      int tempBufferIndex = bufferIndex;
      while (tempLength < length) {
        mpiBuffer = buffers.get(tempBufferIndex);
        messageBuffers.add(mpiBuffer);
        tempLength += mpiBuffer.getByteBuffer().remaining();
        tempBufferIndex++;
//        LOG.info(String.format("%d temp %d length %d readLength %d",
//            executor, tempLength, length, readLength));
      }

      Object object = buildMessage(currentMessage.getType(), messageBuffers, length);
      readLength += length + 6;
      byteBuffer = mpiBuffer.getByteBuffer();
      if (byteBuffer.remaining() > 0) {
        bufferIndex = tempBufferIndex - 1;
      } else {
        bufferIndex = tempBufferIndex;
      }

      KeyedContent keyedContent =  new KeyedContent(source, object);
      returnList.add(keyedContent);
    }
    return returnList;
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

  private Object buildMessage(MessageType type, List<MPIBuffer> message, int length) {
    switch (type) {
      case INTEGER:
        break;
      case LONG:
        break;
      case DOUBLE:
        break;
      case OBJECT:
        return buildObject(message, length);
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

  private Object buildObject(List<MPIBuffer> buffers, int length) {
    MPIByteArrayInputStream input = null;
    try {
      input = new MPIByteArrayInputStream(buffers, length);
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
