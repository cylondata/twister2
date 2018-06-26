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
import java.util.Queue;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.MessageFlags;
import edu.iu.dsc.tws.comms.api.MessageHeader;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.dfw.ChannelMessage;
import edu.iu.dsc.tws.comms.dfw.DataBuffer;
import edu.iu.dsc.tws.comms.dfw.OutMessage;
import edu.iu.dsc.tws.comms.dfw.io.types.DataSerializer;
import edu.iu.dsc.tws.comms.dfw.io.types.KeySerializer;
import edu.iu.dsc.tws.comms.utils.KryoSerializer;

public class SingleMessageSerializer implements MessageSerializer {
  private static final Logger LOG = Logger.getLogger(SingleMessageSerializer.class.getName());

  private Queue<DataBuffer> sendBuffers;
  private KryoSerializer serializer;
  private Config config;
  private boolean keyed;
  private int executor;

  private static final int HEADER_SIZE = 16;
  // we need to put the message length and key length if keyed message
  private static final int MAX_SUB_MESSAGE_HEADER_SPACE = 4 + 4;
  // for s normal message we only put the length
  private static final int NORMAL_SUB_MESSAGE_HEADER_SIZE = 4;

  public SingleMessageSerializer(KryoSerializer kryoSerializer) {
    this.serializer = kryoSerializer;
  }

  @Override
  public void init(Config cfg, Queue<DataBuffer> buffers, boolean k) {
    this.config = cfg;
    this.sendBuffers = buffers;
    this.keyed = k;
  }

  @Override
  public Object build(Object message, Object partialBuildObject) {
    OutMessage sendMessage = (OutMessage) partialBuildObject;
    // we got an already serialized message, lets just return it
    if (sendMessage.getMPIMessage().isComplete()) {
      sendMessage.setSendState(OutMessage.SendState.SERIALIZED);
      return sendMessage;
    }

    if (sendMessage.getSerializationState() == null) {
      sendMessage.setSerializationState(new SerializeState());
    }

    while (sendBuffers.size() > 0 && sendMessage.serializedState()
        != OutMessage.SendState.SERIALIZED) {
      DataBuffer buffer = sendBuffers.poll();

      if (buffer == null) {
        break;
      }

      if (sendMessage.serializedState() == OutMessage.SendState.INIT
          || sendMessage.serializedState() == OutMessage.SendState.SENT_INTERNALLY) {
        // build the header
        buildHeader(buffer, sendMessage);
        sendMessage.setSendState(OutMessage.SendState.HEADER_BUILT);
      }

      if (sendMessage.serializedState() == OutMessage.SendState.HEADER_BUILT
          || sendMessage.serializedState() == OutMessage.SendState.BODY_BUILT) {
        if ((sendMessage.getFlags() & MessageFlags.EMPTY) == MessageFlags.EMPTY) {
          sendMessage.setSendState(OutMessage.SendState.SERIALIZED);
          sendMessage.getSerializationState().setTotalBytes(0);
        } else {
          // build the body
          // first we need to serialize the body if needed
          boolean complete = serializeBody(message, sendMessage, buffer);
          if (complete) {
            sendMessage.setSendState(OutMessage.SendState.SERIALIZED);
          }
        }
      }
      // okay we are adding this buffer
      sendMessage.getMPIMessage().addBuffer(buffer);
      if (sendMessage.serializedState() == OutMessage.SendState.SERIALIZED) {
        ChannelMessage channelMessage = sendMessage.getMPIMessage();
        SerializeState state = sendMessage.getSerializationState();
        int totalBytes = state.getTotalBytes();
        channelMessage.getBuffers().get(0).getByteBuffer().putInt(12, totalBytes);

        MessageHeader.Builder builder = MessageHeader.newBuilder(sendMessage.getSource(),
            sendMessage.getEdge(), totalBytes);
        builder.destination(sendMessage.getDestintationIdentifier());
        sendMessage.getMPIMessage().setHeader(builder.build());
        state.setTotalBytes(0);

        // mark the original message as complete
        channelMessage.setComplete(true);
      } else {
        LOG.fine("Message NOT FULLY serialized");
      }
    }
    return sendMessage;
  }

  private void buildHeader(DataBuffer buffer, OutMessage sendMessage) {
    if (buffer.getCapacity() < HEADER_SIZE) {
      throw new RuntimeException("The buffers should be able to hold the complete header");
    }
    ByteBuffer byteBuffer = buffer.getByteBuffer();
    // now lets put the content of header in
    byteBuffer.putInt(sendMessage.getSource());
    // the path we are on, if not grouped it will be 0 and ignored
    byteBuffer.putInt(sendMessage.getFlags());
    byteBuffer.putInt(sendMessage.getDestintationIdentifier());
    // we add 0 for now and late change it
    byteBuffer.putInt(0);
    // at this point we haven't put the length and we will do it at the serialization
    sendMessage.setWrittenHeaderSize(HEADER_SIZE);
    // lets set the size for 16 for now
    buffer.setSize(16);
  }

  /**
   * Serialized the message into the buffer
   *
   * @return true if the message is completely written
   */
  private boolean serializeBody(Object payload,
                                OutMessage sendMessage, DataBuffer buffer) {
    MessageType type = sendMessage.getMPIMessage().getType();
    if (type == MessageType.OBJECT || type == MessageType.INTEGER || type == MessageType.LONG
        || type == MessageType.DOUBLE || type == MessageType.BYTE || type == MessageType.STRING
        || type == MessageType.MULTI_FIXED_BYTE) {
      if (!keyed) {
        return serializeData(payload, sendMessage.getSerializationState(), buffer, type);
      } else {
        KeyedContent keyedContent = (KeyedContent) payload;
        return serializeKeyedData(keyedContent.getValue(), keyedContent.getKey(),
            sendMessage.getSerializationState(), buffer, type, keyedContent.getKeyType());
      }
    }
    if (type == MessageType.BUFFER) {
      return serializeBuffer(payload, sendMessage, buffer);
    }
    return false;
  }

  private boolean serializeKeyedData(Object content, Object key, SerializeState state,
                                     DataBuffer targetBuffer, MessageType contentType,
                                     MessageType keyType) {
    ByteBuffer byteBuffer = targetBuffer.getByteBuffer();
    // okay we need to serialize the header
    if (state.getPart() == SerializeState.Part.INIT) {
      int keyLength = KeySerializer.serializeKey(key,
          keyType, state, serializer);
      // okay we need to serialize the data
      int dataLength = DataSerializer.serializeData(content,
          contentType, state, serializer);
    }

    if (state.getPart() == SerializeState.Part.INIT
        || state.getPart() == SerializeState.Part.HEADER) {
      boolean complete = KeySerializer.copyKeyToBuffer(key,
          keyType, targetBuffer.getByteBuffer(), state, serializer);
      if (complete) {
        state.setPart(SerializeState.Part.BODY);
      } else {
        state.setPart(SerializeState.Part.HEADER);
      }
    }

    // now we can serialize the body
    if (state.getPart() != SerializeState.Part.BODY) {
      return false;
    }

    boolean completed = DataSerializer.copyDataToBuffer(content,
        contentType, byteBuffer, state, serializer);
    // now set the size of the buffer
    targetBuffer.setSize(byteBuffer.position());

    // okay we are done with the message
    if (completed) {
      // add the key size at the end to total size
      state.setBytesCopied(0);
      state.setBufferNo(0);
      state.setData(null);
      state.setPart(SerializeState.Part.INIT);
      state.setKeySize(0);
      return true;
    } else {
      return false;
    }
  }

  private boolean serializeData(Object content, SerializeState state,
                                DataBuffer targetBuffer, MessageType messageType) {
    ByteBuffer byteBuffer = targetBuffer.getByteBuffer();
    // okay we need to serialize the header
    if (state.getPart() == SerializeState.Part.INIT) {
      // okay we need to serialize the data
      int dataLength = DataSerializer.serializeData(content, messageType, state, serializer);
      // add the header bytes to the total bytes
      state.setPart(SerializeState.Part.BODY);
    }

    // now we can serialize the body
    if (state.getPart() != SerializeState.Part.BODY) {
      return false;
    }

    boolean completed = DataSerializer.copyDataToBuffer(content,
        messageType, byteBuffer, state, serializer);
    // now set the size of the buffer
    targetBuffer.setSize(byteBuffer.position());

    // okay we are done with the message
    if (completed) {
      // add the key size at the end to total size
      state.setBytesCopied(0);
      state.setBufferNo(0);
      state.setData(null);
      state.setPart(SerializeState.Part.INIT);
      state.setKeySize(0);
      return true;
    } else {
      return false;
    }
  }

  private boolean serializeBuffer(Object object, OutMessage sendMessage, DataBuffer buffer) {
    DataBuffer dataBuffer = (DataBuffer) object;
    ByteBuffer byteBuffer = buffer.getByteBuffer();
    if (sendMessage.serializedState() == OutMessage.SendState.HEADER_BUILT) {
      // okay we need to serialize the data
      // at this point we know the length of the data
      byteBuffer.putInt(12, dataBuffer.getSize());
      // now lets set the header
      MessageHeader.Builder builder = MessageHeader.newBuilder(sendMessage.getSource(),
          sendMessage.getEdge(), dataBuffer.getSize());
      builder.destination(sendMessage.getDestintationIdentifier());
      sendMessage.getMPIMessage().setHeader(builder.build());
    }
    buffer.setSize(16 + dataBuffer.getSize());
    // okay we are done with the message
    sendMessage.setSendState(OutMessage.SendState.SERIALIZED);
    return true;
  }
}
