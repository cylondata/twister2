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

  public SingleMessageSerializer(KryoSerializer kryoSerializer) {
    this.serializer = kryoSerializer;
  }

  @Override
  public void init(Config cfg, Queue<DataBuffer> buffers, boolean k) {
    this.config = cfg;
    this.sendBuffers = buffers;
    this.keyed = k;
  }

  /**
   * Builds the message and copies it into data buffers.The structure of the message depends on the
   * type of message that is sent, for example if it is a keyed message or not. The main structure
   * of the built message is |Header|Body|. The header has the following structure
   * |source|flags|destinationID|length|, where length is the length of the body. For non-keyed
   * message the body will simply be the set of bytes that represent the message. for keyed messages
   * the key information is also added. So the body has the following structure.
   * |(Key Size) + Key|Body|. The key size is optional since the size of the key is only added to
   * the buffer if it is not a primitive type.
   *
   * @param message the message object that needs to be built and copied into data buffers
   * @param partialBuildObject the sendMessage object that contains meta data that is needed and is
   * used to route the message properly. the send message will contain the data buffers that keep
   * the built message
   * @return the sendMessage(or partialBuildObject) that contains the current state of the message
   * including the message data buffers.
   */
  @Override
  public Object build(Object message, Object partialBuildObject) {
    OutMessage sendMessage = (OutMessage) partialBuildObject;
    // we got an already serialized message, lets just return it
    if (sendMessage.getChannelMessage().isComplete()) {
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
          || sendMessage.serializedState() == OutMessage.SendState.BODY_BUILT
          || sendMessage.serializedState() == OutMessage.SendState.PARTIALLY_SERIALIZED) {
        if ((sendMessage.getFlags() & MessageFlags.END) == MessageFlags.END) {
          sendMessage.setSendState(OutMessage.SendState.SERIALIZED);
          sendMessage.getSerializationState().setTotalBytes(0);
        } else {
          // build the body
          // first we need to serialize the body if needed
          boolean complete = serializeBody(message, sendMessage, buffer);
          if (complete) {
            sendMessage.setSendState(OutMessage.SendState.SERIALIZED);
          } else {
            sendMessage.setSendState(OutMessage.SendState.PARTIALLY_SERIALIZED);
          }
        }
      }
      // okay we are adding this buffer
      sendMessage.getChannelMessage().addBuffer(buffer);
      if (sendMessage.serializedState() == OutMessage.SendState.SERIALIZED) {
        ChannelMessage channelMessage = sendMessage.getChannelMessage();
        SerializeState state = sendMessage.getSerializationState();
        if (!channelMessage.isHeaderSent()) {
          int totalBytes = state.getTotalBytes();
          channelMessage.getBuffers().get(0).getByteBuffer().putInt(HEADER_SIZE - Integer.BYTES,
              totalBytes);


          MessageHeader.Builder builder = MessageHeader.newBuilder(sendMessage.getSource(),
              sendMessage.getEdge(), totalBytes);
          builder.destination(sendMessage.getPath());
          channelMessage.setHeader(builder.build());
          state.setTotalBytes(0);
          channelMessage.setHeaderSent(true);
        }
        // mark the original message as complete
        channelMessage.setComplete(true);
      } else if (sendMessage.serializedState() == OutMessage.SendState.PARTIALLY_SERIALIZED) {
        ChannelMessage channelMessage = sendMessage.getChannelMessage();
        SerializeState state = sendMessage.getSerializationState();
        if (!channelMessage.isHeaderSent()) {
          int totalBytes = state.getCurretHeaderLength();
          channelMessage.getBuffers().get(0).getByteBuffer().putInt(HEADER_SIZE - Integer.BYTES,
              totalBytes);

          MessageHeader.Builder builder = MessageHeader.newBuilder(sendMessage.getSource(),
              sendMessage.getEdge(), totalBytes);
          builder.destination(sendMessage.getPath());
          channelMessage.setHeader(builder.build());
          channelMessage.setHeaderSent(true);
        }
        state.setTotalBytes(0);
        LOG.fine("Message Partially serialized");

      } else {
        LOG.fine("Message NOT FULLY serialized");
      }
    }
    return sendMessage;
  }

  /**
   * Builds the header of the message. The length value is inserted later so 0 is added as a place
   * holder value. The header structure is |source|flags|destinationID|length|
   *
   * @param buffer the buffer to which the header is placed
   * @param sendMessage the message that the header is build for
   */
  private void buildHeader(DataBuffer buffer, OutMessage sendMessage) {
    if (buffer.getCapacity() < HEADER_SIZE) {
      throw new RuntimeException("The buffers should be able to hold the complete header");
    }
    ByteBuffer byteBuffer = buffer.getByteBuffer();
    // now lets put the content of header in
    byteBuffer.putInt(sendMessage.getSource());
    // the path we are on, if not grouped it will be 0 and ignored
    byteBuffer.putInt(sendMessage.getFlags());
    byteBuffer.putInt(sendMessage.getPath());
    // we add 0 for now and late change it
    byteBuffer.putInt(0);
    // at this point we haven't put the length and we will do it at the serialization
    sendMessage.setWrittenHeaderSize(HEADER_SIZE);
    // lets set the size for 16 for now
    buffer.setSize(HEADER_SIZE);
  }

  /**
   * Builds the body of the message. Based on the message type different build methods are called
   *
   * @param payload the message that needs to be built
   * @param sendMessage the send message object that contains all the metadata
   * @param targetBuffer the data targetBuffer to which the built message needs to be copied
   * @return true if the body was built and copied to the targetBuffer successfully,false otherwise.
   */
  private boolean serializeBody(Object payload,
                                OutMessage sendMessage, DataBuffer targetBuffer) {
    MessageType type = sendMessage.getChannelMessage().getType();
    if ((sendMessage.getFlags() & MessageFlags.BARRIER) == MessageFlags.BARRIER) {
      return serializeData(payload, sendMessage.getSerializationState(), targetBuffer, type);
    } else {
      if (type == MessageType.OBJECT || type == MessageType.INTEGER || type == MessageType.LONG
          || type == MessageType.DOUBLE || type == MessageType.BYTE || type == MessageType.STRING
          || type == MessageType.MULTI_FIXED_BYTE) {
        if (!keyed) {
          return serializeData(payload, sendMessage.getSerializationState(), targetBuffer, type);
        } else {
          KeyedContent keyedContent = (KeyedContent) payload;
          return serializeKeyedData(keyedContent.getValue(), keyedContent.getKey(),
              sendMessage.getSerializationState(), targetBuffer, type, keyedContent.getKeyType());
        }
      } else if (type == MessageType.BUFFER) {
        return serializeBuffer(payload, sendMessage, targetBuffer);
      }
    }
    return false;
  }

  /**
   * Helper method that builds the body of the message for keyed messages.
   *
   * @param payload the message that needs to be built
   * @param key the key associated with the message
   * @param state the state object of the message
   * @param targetBuffer the data targetBuffer to which the built message needs to be copied
   * @param messageType the type of the message data
   * @param keyType the type of the message key
   * @return true if the body was built and copied to the targetBuffer successfully,false otherwise.
   */
  private boolean serializeKeyedData(Object payload, Object key, SerializeState state,
                                     DataBuffer targetBuffer, MessageType messageType,
                                     MessageType keyType) {
    ByteBuffer byteBuffer = targetBuffer.getByteBuffer();
    // okay we need to serialize the header
    if (state.getPart() == SerializeState.Part.INIT) {
      int keyLength = KeySerializer.serializeKey(key,
          keyType, state, serializer);
      // okay we need to serialize the data
      int dataLength = DataSerializer.serializeData(payload,
          messageType, state, serializer);
      state.setCurretHeaderLength(dataLength + keyLength);
    }

    if (state.getPart() == SerializeState.Part.INIT
        || state.getPart() == SerializeState.Part.HEADER) {
      if (key instanceof byte[]) {
        LOG.info("Byte message with size: " + ((byte[]) key).length);
      }
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

    boolean completed = DataSerializer.copyDataToBuffer(payload,
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

  /**
   * Helper method that builds the body of the message for regular messages.
   *
   * @param payload the message that needs to be built
   * @param state the state object of the message
   * @param targetBuffer the data targetBuffer to which the built message needs to be copied
   * @param messageType the type of the message data
   * @return true if the body was built and copied to the targetBuffer successfully,false otherwise.
   */
  private boolean serializeData(Object payload, SerializeState state,
                                DataBuffer targetBuffer, MessageType messageType) {
    ByteBuffer byteBuffer = targetBuffer.getByteBuffer();
    // okay we need to serialize the header
    if (state.getPart() == SerializeState.Part.INIT) {
      // okay we need to serialize the data
      int dataLength = DataSerializer.serializeData(payload, messageType, state, serializer);
      state.setCurretHeaderLength(dataLength);
      // add the header bytes to the total bytes
      state.setPart(SerializeState.Part.BODY);
    }

    // now we can serialize the body
    if (state.getPart() != SerializeState.Part.BODY) {
      return false;
    }

    boolean completed = DataSerializer.copyDataToBuffer(payload,
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

  /**
   * Helper method that builds the body of the message for targetBuffer type messages.
   *
   * @param payload the message that needs to be built
   * @param sendMessage the send message object that contains all the metadata
   * @param targetBuffer the data targetBuffer to which the built message needs to be copied
   * @return true if the body was built and copied to the targetBuffer successfully,false otherwise.
   */
  private boolean serializeBuffer(Object payload, OutMessage sendMessage, DataBuffer targetBuffer) {
    DataBuffer dataBuffer = (DataBuffer) payload;
    ByteBuffer byteBuffer = targetBuffer.getByteBuffer();
    if (sendMessage.serializedState() == OutMessage.SendState.HEADER_BUILT) {
      // okay we need to serialize the data
      // at this point we know the length of the data
      byteBuffer.putInt(HEADER_SIZE - Integer.BYTES, dataBuffer.getSize());
      // now lets set the header
      MessageHeader.Builder builder = MessageHeader.newBuilder(sendMessage.getSource(),
          sendMessage.getEdge(), dataBuffer.getSize());
      builder.destination(sendMessage.getPath());
      sendMessage.getChannelMessage().setHeader(builder.build());
    }
    targetBuffer.setSize(HEADER_SIZE + dataBuffer.getSize());
    // okay we are done with the message
    sendMessage.setSendState(OutMessage.SendState.SERIALIZED);
    return true;
  }
}
