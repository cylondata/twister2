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
import java.util.logging.Logger;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.MessageHeader;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.dfw.ChannelMessage;
import edu.iu.dsc.tws.comms.dfw.DataBuffer;
import edu.iu.dsc.tws.comms.dfw.io.types.DataDeserializer;
import edu.iu.dsc.tws.comms.dfw.io.types.KeyDeserializer;
import edu.iu.dsc.tws.comms.utils.KryoSerializer;
import edu.iu.dsc.tws.comms.utils.MessageTypeUtils;

public class MultiMessageDeserializer implements MessageDeSerializer {
  private static final Logger LOG = Logger.getLogger(MultiMessageDeserializer.class.getName());

  private KryoSerializer serializer;

  private int executor;

  private boolean keyed;

  public MultiMessageDeserializer(KryoSerializer kryoSerializer, int exec) {
    this.serializer = kryoSerializer;
    this.executor = exec;
  }

  @Override
  public void init(Config cfg, boolean k) {
    this.keyed = k;
  }

  /**
   * Builds the message from the data buffers in the partialObject. Since this method
   * supports multi-messages it iterates through the buffers and builds all the messages separately
   *
   * @param partialObject message object that needs to be built
   * @param edge the edge value associated with this message
   * @return the built message as a list of objects
   */
  @Override
  public Object build(Object partialObject, int edge) {
    ChannelMessage currentMessage = (ChannelMessage) partialObject;
    int readLength = 0;
    int bufferIndex = 0;
    List<DataBuffer> buffers = currentMessage.getBuffers();
    List<Object> returnList = new ArrayList<>();
    MessageHeader header = currentMessage.getHeader();

    if (header == null) {
      throw new RuntimeException("Header must be built before the message");
    }
    while (readLength < header.getLength()) {
      List<DataBuffer> messageBuffers = new ArrayList<>();
      DataBuffer dataBuffer = buffers.get(bufferIndex);
      ByteBuffer byteBuffer = dataBuffer.getByteBuffer();
      // now read the length
      int length = byteBuffer.getInt();
      int tempLength = 0;
      int tempBufferIndex = bufferIndex;
      while (tempLength < length) {
        dataBuffer = buffers.get(tempBufferIndex);
        messageBuffers.add(dataBuffer);
        tempLength += dataBuffer.getByteBuffer().remaining();
        tempBufferIndex++;
      }

      Object object = buildMessage(currentMessage, messageBuffers, length);
      readLength += length + Integer.BYTES;
      if (keyed && !MessageTypeUtils.isPrimitiveType(currentMessage.getKeyType())) {
        //adding 4 to the length since the key length is also kept
        readLength += Integer.BYTES;
        if (MessageTypeUtils.isMultiMessageType(currentMessage.getKeyType())) {
          readLength += Integer.BYTES;
        }
      }
      byteBuffer = dataBuffer.getByteBuffer();
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
    ChannelMessage currentMessage = (ChannelMessage) partialObject;
    int readLength = 0;
    int bufferIndex = 0;
    List<DataBuffer> buffers = currentMessage.getBuffers();
    List<Object> returnList = new ArrayList<>();
    MessageHeader header = currentMessage.getHeader();

    if (header == null) {
      throw new RuntimeException("Header must be built before the message");
    }
    while (readLength < header.getLength()) {
      List<DataBuffer> messageBuffers = new ArrayList<>();
      DataBuffer dataBuffer = buffers.get(bufferIndex);
      ByteBuffer byteBuffer = dataBuffer.getByteBuffer();
      // now read the length
      int length = byteBuffer.getInt();
      int tempLength = 0;
      int tempBufferIndex = bufferIndex;
      while (tempLength < length) {
        dataBuffer = buffers.get(tempBufferIndex);
        messageBuffers.add(dataBuffer);
        tempLength += dataBuffer.getByteBuffer().remaining();
        tempBufferIndex++;
      }

      Object object = getSingleDataBuffers(currentMessage, messageBuffers, length);
      readLength += length + Integer.BYTES;
      if (keyed && !MessageTypeUtils.isPrimitiveType(currentMessage.getKeyType())) {
        //adding 4 to the length since the key length is also kept
        readLength += Integer.BYTES;
      }
      byteBuffer = dataBuffer.getByteBuffer();
      if (byteBuffer.remaining() > 0) {
        bufferIndex = tempBufferIndex - 1;
      } else {
        bufferIndex = tempBufferIndex;
      }

      returnList.add(object);
    }
    return returnList;
  }

  private Object getSingleDataBuffers(ChannelMessage channelMessage,
                                      List<DataBuffer> message, int length) {
    MessageType type = channelMessage.getType();

    if (!keyed) {
      return DataDeserializer.getAsByteArray(message,
          length, type);
    } else {
      Pair<Integer, Object> keyPair = KeyDeserializer.
          getKeyAsByteArray(channelMessage.getKeyType(),
              message);
      byte[] data = DataDeserializer.getAsByteArray(message,
          length - keyPair.getKey(), type);
      return new ImmutablePair<>(keyPair.getValue(), data);
    }
  }

  /**
   * Builds the header object from the data in the data buffer
   *
   * @param buffer data buffer that contains the message
   * @param edge the edge value associated with this message
   * @return the built message header object
   */
  @Override
  public MessageHeader buildHeader(DataBuffer buffer, int edge) {
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

  /**
   * Builds the message from the data in the data buffers.
   *
   * @param message the object that contains all the message details and data buffers
   * @return the built message object
   */
  private Object buildMessage(ChannelMessage channelMessage, List<DataBuffer> message, int length) {
    MessageType type = channelMessage.getType();
    if (keyed) {
      Pair<Integer, Object> keyPair = KeyDeserializer.deserializeKey(channelMessage.getKeyType(),
          message, serializer);
      Object data;
      if (MessageTypeUtils.isMultiMessageType(channelMessage.getKeyType())) {
        //TODO :need to check correctness for multi-message
        data = DataDeserializer.deserializeData(message,
            length - keyPair.getKey(), serializer, type,
            ((List) keyPair.getValue()).size());
      } else {
        data = DataDeserializer.deserializeData(message, length - keyPair.getKey(),
            serializer, type);
      }
      return new KeyedContent(keyPair.getValue(), data,
          channelMessage.getKeyType(), channelMessage.getType());
    } else {
      return DataDeserializer.deserializeData(message, length, serializer, type);
    }
  }
}
