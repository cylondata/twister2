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
import java.util.List;
import java.util.Queue;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.MessageFlags;
import edu.iu.dsc.tws.comms.api.MessageHeader;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.dfw.ChannelMessage;
import edu.iu.dsc.tws.comms.dfw.DataBuffer;
import edu.iu.dsc.tws.comms.dfw.MessageDirection;
import edu.iu.dsc.tws.comms.dfw.OutMessage;
import edu.iu.dsc.tws.comms.dfw.io.types.DataSerializer;
import edu.iu.dsc.tws.comms.dfw.io.types.KeySerializer;
import edu.iu.dsc.tws.comms.utils.KryoSerializer;

public class UnifiedSerializer implements MessageSerializer {
  private static final Logger LOG = Logger.getLogger(UnifiedSerializer.class.getName());

  public static final int HEADER_SIZE = 16;
  // we need to put the message length and key length if keyed message
  public static final int MAX_SUB_MESSAGE_HEADER_SPACE = 4 + 4;
  // for s normal message we only put the length
  public static final int NORMAL_SUB_MESSAGE_HEADER_SIZE = 4;

  /**
   * The DataBuffers available
   */
  private Queue<DataBuffer> sendBuffers;

  /**
   * The kryo serializer to be used for objects
   */
  private KryoSerializer serializer;


  /**
   * Weather this is a keyed message
   */
  private boolean keyed;

  /**
   * The executor
   */
  private int executor;


  public UnifiedSerializer(KryoSerializer serializer, int executor) {
    this.executor = executor;
    this.serializer = serializer;
  }

  @Override
  public void init(Config cfg, Queue<DataBuffer> buffers, boolean k) {
    this.sendBuffers = buffers;
    this.keyed = k;
  }

  @Override
  public Object build(Object data, Object partialBuildObject) {
    OutMessage sendMessage = (OutMessage) partialBuildObject;
    // we got an already serialized message, lets just return it
    ChannelMessage channelMessage = new ChannelMessage(sendMessage.getSource(),
        sendMessage.getDataType(), MessageDirection.OUT, sendMessage.getReleaseCallback());
    buildHeader(sendMessage, channelMessage, 0);

    // we loop until everything is serialized
    while (sendBuffers.size() > 0
        && sendMessage.getSendState() != OutMessage.SendState.SERIALIZED) {

      // we can continue only if there is a data buffer
      DataBuffer buffer = sendBuffers.poll();
      if (buffer == null) {
        break;
      }

      // this is the first time we are seeing this message
      if (sendMessage.getSendState() == OutMessage.SendState.INIT
          || sendMessage.getSendState() == OutMessage.SendState.SENT_INTERNALLY) {
        // we set the state here, because we can set it to serialized below
        sendMessage.setSendState(OutMessage.SendState.HEADER_BUILT);
        // build the header
        if (data instanceof List) {
          // for list message we need to put the size of the list
          DFWIOUtils.buildHeader(buffer, sendMessage, ((List) data).size());
          buildHeader(sendMessage, channelMessage, ((List) data).size());
        } else {
          if ((sendMessage.getFlags() & MessageFlags.END) == MessageFlags.END) {
            sendMessage.setSendState(OutMessage.SendState.SERIALIZED);
            // we set the number of messages to 0, only header will be sent
            DFWIOUtils.buildHeader(buffer, sendMessage, 0);
            buildHeader(sendMessage, channelMessage, 0);
          } else {
            // for single message we need to put the size as 1
            DFWIOUtils.buildHeader(buffer, sendMessage, -1);
            buildHeader(sendMessage, channelMessage, -1);
          }
        }
      }

      // okay we have a body to build and it is not done fully yet
      if (sendMessage.getSendState() == OutMessage.SendState.HEADER_BUILT
          || sendMessage.getSendState() == OutMessage.SendState.PARTIALLY_SERIALIZED) {
        sendMessage.setSendState(OutMessage.SendState.PARTIALLY_SERIALIZED);
        serializeBody(data, sendMessage, buffer);
      }

      // okay we are adding this buffer
      channelMessage.addBuffer(buffer);
      if (sendMessage.getSendState() == OutMessage.SendState.SERIALIZED) {
        channelMessage.setComplete(true);
      }
    }

    // if we didn't do anything lets return null
    if (channelMessage.getBuffers().size() == 0) {
      return null;
    }

    return channelMessage;
  }

  /**
   * Build the header to set for channel messages laters
   * @param sendMessage messages
   * @param channelMessage channel message
   * @param numMessages number of messages
   */
  private void buildHeader(OutMessage sendMessage, ChannelMessage channelMessage,
                           int numMessages) {
    MessageHeader.Builder builder = MessageHeader.newBuilder(sendMessage.getSource(),
        sendMessage.getEdge(), numMessages);
    builder.destination(sendMessage.getPath());
    channelMessage.setHeader(builder.build());
  }

  /**
   * Builds the body of the message. Based on the message type different build methods are called
   *
   * @param payload the message that needs to be built, this is assumed to be a List that contains
   * several message objects that need to be built
   * @param sendMessage the send message object that contains all the metadata
   * @param targetBuffer the data buffer to which the built message needs to be copied
   */
  @SuppressWarnings("rawtypes")
  private void serializeBody(Object payload, OutMessage sendMessage, DataBuffer targetBuffer) {
    // if serialized nothing to do
    if (sendMessage.getSendState() == OutMessage.SendState.SERIALIZED) {
      return;
    }

    SerializeState state = sendMessage.getSerializationState();

    // we assume remaining = capacity of the targetBuffer as we always get a fresh targetBuffer her
    int remaining = targetBuffer.getByteBuffer().remaining();
    // we cannot use this targetBuffer as we cannot put the sub header
    if (remaining <= MAX_SUB_MESSAGE_HEADER_SPACE) {
      throw new RuntimeException("This targetBuffer is too small to fit a message: " + remaining);
    }

    if (payload instanceof List) {
      List objectList = (List) payload;
      int startIndex = state.getCurrentObject();
      // we will copy until we have space left or we are have serialized all the objects
      for (int i = startIndex; i < objectList.size(); i++) {
        Object o = objectList.get(i);
        boolean complete = serializeSingleMessage(o, sendMessage, targetBuffer);
        if (complete) {
          state.setCurretHeaderLength(state.getTotalBytes());
          state.setCurrentObject(i + 1);
        } else {
          break;
        }

        // check how much space left in this targetBuffer
        remaining = targetBuffer.getByteBuffer().remaining();
        // if we have less than this amount of space, that means we may not be able to put the next
        // header in a contigous space, so we cannot use this targetBuffer anymore
        if (!(remaining > MAX_SUB_MESSAGE_HEADER_SPACE
            && state.getCurrentObject() < objectList.size())) {
          break;
        }
      }

      // we have serialized all the objects
      if (state.getCurrentObject() == objectList.size()) {
        sendMessage.setSendState(OutMessage.SendState.SERIALIZED);
      } else {
        sendMessage.setSendState(OutMessage.SendState.PARTIALLY_SERIALIZED);
      }
    } else {
      boolean complete = serializeSingleMessage(payload, sendMessage, targetBuffer);
      if (complete) {
        sendMessage.setSendState(OutMessage.SendState.SERIALIZED);
      }
    }
  }

  /**
   * Builds the body of the message. Based on the message type different build methods are called
   *
   * @param payload the message that needs to be built
   * @param sendMessage the send message object that contains all the metadata
   * @param targetBuffer the data targetBuffer to which the built message needs to be copied
   * @return true if the body was built and copied to the targetBuffer successfully,false otherwise.
   */
  private boolean serializeSingleMessage(Object payload,
                                OutMessage sendMessage, DataBuffer targetBuffer) {
    MessageType type = sendMessage.getDataType();
    if (!keyed) {
      return serializeData(payload, sendMessage.getSerializationState(), targetBuffer, type);
    } else {
      Tuple tuple = (Tuple) payload;
      return serializeKeyedData(tuple.getValue(), tuple.getKey(),
          sendMessage.getSerializationState(), targetBuffer, type, tuple.getKeyType());
    }
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
      state.setPart(SerializeState.Part.HEADER);
    }

    if (state.getPart() == SerializeState.Part.HEADER) {
      // first we need to copy the data size to buffer
      if (buildSubMessageHeader(targetBuffer, state.getCurretHeaderLength())) {
        return false;
      }
      // todo: what happens if we cannot copy key
      // this call will copy the key length to buffer as well
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

    // now lets copy the actual data
    boolean completed = DataSerializer.copyDataToBuffer(payload,
        messageType, byteBuffer, state, serializer);
    // now set the size of the buffer
    targetBuffer.setSize(byteBuffer.position());

    // okay we are done with the message
    return DFWIOUtils.resetState(state, completed);
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
      state.setPart(SerializeState.Part.HEADER);
    }

    if (state.getPart() == SerializeState.Part.HEADER) {
      // first we need to copy the data size to buffer
      if (buildSubMessageHeader(targetBuffer, state.getCurretHeaderLength())) {
        return false;
      }
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
    return DFWIOUtils.resetState(state, completed);
  }

  /**
   * Builds the sub message header which is used in multi messages to identify the lengths of each
   * sub message. The structure of the sub message header is |length + (key length)|. The key length
   * is added for keyed messages
   */
  private boolean buildSubMessageHeader(DataBuffer buffer, int length) {
    ByteBuffer byteBuffer = buffer.getByteBuffer();
    if (byteBuffer.remaining() < NORMAL_SUB_MESSAGE_HEADER_SIZE) {
      return true;
    }
    byteBuffer.putInt(length);
    return false;
  }
}
