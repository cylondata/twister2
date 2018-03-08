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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.MessageHeader;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.mpi.MPIBuffer;
import edu.iu.dsc.tws.comms.mpi.MPIMessage;
import edu.iu.dsc.tws.comms.mpi.io.types.DataDeserializer;
import edu.iu.dsc.tws.comms.mpi.io.types.KeyDeserializer;
import edu.iu.dsc.tws.comms.utils.KryoSerializer;
import edu.iu.dsc.tws.comms.utils.MessageTypeUtils;

public class MPIMultiMessageDeserializer implements MessageDeSerializer {
  private static final Logger LOG = Logger.getLogger(MPIMultiMessageDeserializer.class.getName());

  private KryoSerializer serializer;

  private int executor;

  private boolean keyed;

  public MPIMultiMessageDeserializer(KryoSerializer kryoSerializer, int exec) {
    this.serializer = kryoSerializer;
    this.executor = exec;
  }

  @Override
  public void init(Config cfg, boolean k) {
    this.keyed = k;
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
//    LOG.info(String.format("%d deserilizing message", executor));
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
//        LOG.info(String.format("%d temp %d length %d readLength %d header %d buf_pos %d",
//            executor, tempLength, length, readLength, header.getLength(),
//            mpiBuffer.getByteBuffer().position()));
      }

      Object object = buildMessage(currentMessage, messageBuffers, length);
      readLength += length + 4;
      if (keyed && !MessageTypeUtils.isPrimitiveType(currentMessage.getKeyType())) {
        //adding 4 to the length since the key length is also kept
        readLength += 4;
        if (MessageTypeUtils.isMultiMessageType(currentMessage.getKeyType())) {
          readLength += 4;
        }
      }
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
  public Object getDataBuffers(Object partialObject, int edge) {
    MPIMessage currentMessage = (MPIMessage) partialObject;
    int readLength = 0;
    int bufferIndex = 0;
    List<MPIBuffer> buffers = currentMessage.getBuffers();
    List<Object> returnList = new ArrayList<>();
    MessageHeader header = currentMessage.getHeader();

    if (header == null) {
      throw new RuntimeException("Header must be built before the message");
    }
//    LOG.info(String.format("%d deserilizing message", executor));
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
//        LOG.info(String.format("%d temp %d length %d readLength %d header %d buf_pos %d",
//            executor, tempLength, length, readLength, header.getLength(),
//            mpiBuffer.getByteBuffer().position()));
      }

      Object object = getSingleDataBuffers(currentMessage, messageBuffers, length);
      readLength += length + 4;
      if (keyed && !MessageTypeUtils.isPrimitiveType(currentMessage.getKeyType())) {
        //adding 4 to the length since the key length is also kept
        readLength += 4;
      }
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

  public Object getSingleDataBuffers(MPIMessage mpiMessage, List<MPIBuffer> message, int length) {
    MessageType type = mpiMessage.getType();

    if (!keyed) {
      return DataDeserializer.getAsByteBuffer(message,
          length, type);
    } else {
      Pair<Integer, Object> keyPair = KeyDeserializer.
          getKeyAsByteBuffer(mpiMessage.getKeyType(),
              message);
      byte[] data = DataDeserializer.getAsByteBuffer(message,
          length - keyPair.getKey(), type);
      return new ImmutablePair<>(keyPair.getValue(), data);
    }
  }

  @Override
  public MessageHeader buildHeader(MPIBuffer buffer, int edge) {
//   LOG.info(String.format("%d read header pos: %d", executor, buffer.getByteBuffer().position()));
    int sourceId = buffer.getByteBuffer().getInt();
    int flags = buffer.getByteBuffer().getInt();
    int destId = buffer.getByteBuffer().getInt();
    int length = buffer.getByteBuffer().getInt();

//    if ((flags & MessageFlags.FLAGS_LAST) == MessageFlags.FLAGS_LAST) {
//      LOG.info(String.format("%d RECV LAST SET %d", executor, flags));
//    }

    MessageHeader.Builder headerBuilder = MessageHeader.newBuilder(
        sourceId, edge, length);
    headerBuilder.flags(flags);
    headerBuilder.destination(destId);
    return headerBuilder.build();
  }

  private Object buildMessage(MPIMessage mpiMessage, List<MPIBuffer> message, int length) {
    MessageType type = mpiMessage.getType();
    if (keyed) {
      Pair<Integer, Object> keyPair = KeyDeserializer.deserializeKey(mpiMessage.getKeyType(),
          message, serializer);
      if (MessageTypeUtils.isMultiMessageType(mpiMessage.getKeyType())) {
        //TODO :need to check correctness for multi-message
        return DataDeserializer.deserializeData(message,
            length - keyPair.getKey(), serializer, type,
            ((List) keyPair.getValue()).size());
      } else {
        return DataDeserializer.deserializeData(message, length - keyPair.getKey(),
            serializer, type);
      }
    } else {
      return DataDeserializer.deserializeData(message, length, serializer, type);
    }
  }
}
