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
    List<Object> returnList = new ArrayList<>();
    MessageHeader header = currentMessage.getHeader();

    if (header == null) {
      throw new RuntimeException("Header must be built before the message");
    }
    LOG.info(String.format("%d deserilizing message", executor));
    while (readLength < header.getLength()) {
      List<MPIBuffer> messageBuffers = new ArrayList<>();
      MPIBuffer mpiBuffer = buffers.get(bufferIndex);
      ByteBuffer byteBuffer = mpiBuffer.getByteBuffer();
      // now read the length
      int length = byteBuffer.getInt();
      int tempLength = 0;
      int tempBufferIndex = bufferIndex;
      while (tempLength < length) {
        mpiBuffer = buffers.get(tempBufferIndex);
        messageBuffers.add(mpiBuffer);
        tempLength += mpiBuffer.getByteBuffer().remaining();
        tempBufferIndex++;
        LOG.info(String.format("%d temp %d length %d readLength %d header %d buf_pos %d",
            executor, tempLength, length, readLength, header.getLength(),
            mpiBuffer.getByteBuffer().position()));
      }

      Object object = buildMessage(currentMessage, messageBuffers, length);
      readLength += length + 4;
      byteBuffer = mpiBuffer.getByteBuffer();
      if (byteBuffer.remaining() > 0) {
        bufferIndex = tempBufferIndex - 1;
      } else {
        bufferIndex = tempBufferIndex;
      }

      returnList.add(object);
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

  private Object buildMessage(MPIMessage mpiMessage, List<MPIBuffer> message, int length) {
    MessageType type = mpiMessage.getType();
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
      case KEYED:
        return buildKeyedMessage(mpiMessage.getKeyType(), type, message, length);
      default:
        break;
    }
    return null;
  }

  private KeyedContent buildKeyedMessage(MessageType keyType, MessageType contentType,
                                         List<MPIBuffer> buffers, int length) {
    int currentIndex = 0;
    int keyLength = 0;
    Object key = null;
    // first we need to read the key type
    switch (keyType) {
      case INTEGER:
        keyLength = 4;
        currentIndex = getReadIndex(buffers, currentIndex, 4);
        key = buffers.get(currentIndex).getByteBuffer().getInt();
        break;
      case SHORT:
        keyLength = 2;
        currentIndex = getReadIndex(buffers, currentIndex, 2);
        key = buffers.get(currentIndex).getByteBuffer().getShort();
        break;
      case LONG:
        keyLength = 8;
        currentIndex = getReadIndex(buffers, currentIndex, 8);
        key = buffers.get(currentIndex).getByteBuffer().getInt();
        break;
      case DOUBLE:
        keyLength = 8;
        currentIndex = getReadIndex(buffers, currentIndex, 8);
        key = buffers.get(currentIndex).getByteBuffer().getInt();
        break;
      case OBJECT:
        currentIndex = getReadIndex(buffers, currentIndex, 4);
        keyLength = buffers.get(currentIndex).getByteBuffer().getInt();
        key = buildObject(buffers, keyLength);
        break;
      case BYTE:
        currentIndex = getReadIndex(buffers, currentIndex, 4);
        keyLength = buffers.get(currentIndex).getByteBuffer().getInt();
        key = readBytes(buffers, keyLength);
        break;
      case STRING:
        currentIndex = getReadIndex(buffers, currentIndex, 4);
        keyLength = buffers.get(currentIndex).getByteBuffer().getInt();
        key = new String(readBytes(buffers, keyLength));
        break;
      default:
        break;
    }

    // now lets read the object
    int objectLength = length - keyLength;
    Object object = buildObject(buffers, objectLength);
    return new KeyedContent(key, object);
  }

  private byte[] readBytes(List<MPIBuffer> buffers, int length) {
    byte[] bytes = new byte[length];
    int currentRead = 0;
    int index = 0;
    while (currentRead < length) {
      ByteBuffer byteBuffer = buffers.get(index).getByteBuffer();
      int remaining = byteBuffer.remaining();
      int needRead = length - currentRead;
      int canRead =  remaining > needRead ? needRead : remaining;
      byteBuffer.get(bytes, currentRead, canRead);
      currentRead += canRead;
      index++;
      if (index >= buffers.size()) {
        throw new RuntimeException("Error in buffer management");
      }
    }
    return bytes;
  }

  private int getReadIndex(List<MPIBuffer> buffers, int currentIndex, int expectedSize) {
    for (int i = currentIndex; i < buffers.size(); i++) {
      ByteBuffer byteBuffer = buffers.get(i).getByteBuffer();
      int remaining = byteBuffer.remaining();
      if (remaining > expectedSize) {
        return i;
      }
    }
    throw new RuntimeException("Something is wrong in the buffer management");
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
