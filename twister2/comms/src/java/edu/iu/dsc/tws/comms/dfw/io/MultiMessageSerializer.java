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
import edu.iu.dsc.tws.comms.dfw.OutMessage;
import edu.iu.dsc.tws.comms.dfw.io.types.DataSerializer;
import edu.iu.dsc.tws.comms.dfw.io.types.KeySerializer;
import edu.iu.dsc.tws.comms.utils.KryoSerializer;

/**
 * Serialize a list of messages into buffers
 */
public class MultiMessageSerializer implements MessageSerializer {
  private static final Logger LOG = Logger.getLogger(MultiMessageSerializer.class.getName());

  private Queue<DataBuffer> sendBuffers;
  private KryoSerializer serializer;
  private int executor;

  private static final int HEADER_SIZE = 16;
  // we need to put the message length and key length if keyed message
  private static final int MAX_SUB_MESSAGE_HEADER_SPACE = 4 + 4;
  // for s normal message we only put the length
  private static final int NORMAL_SUB_MESSAGE_HEADER_SIZE = 4;

  private boolean keyed;

  public MultiMessageSerializer(KryoSerializer kryoSerializer, int exec) {
    this.serializer = kryoSerializer;
    this.executor = exec;
  }

  @Override
  public void init(Config cfg, Queue<DataBuffer> buffers, boolean k) {
    this.sendBuffers = buffers;
    this.keyed = k;
  }

  /**
   * Builds the message and copies it into data buffers.The structure of the message depends on the
   * type of message that is sent, this serves a similar purpose to the build method in
   * the {@link SingleMessageSerializer} but works for multi messages, that is a single message can
   * have multiple messages. The main structure of the built message is
   * |Header|Sub-header|Body|Sub-header|Body|Sub-header|Body|.The main header has the following
   * structure |source|flags|destinationID|length|, where length is the length of the whole body,
   * which consists of several sub-headers and bodies. The structure of the sub message header
   * is |length + (key length)|. The key length is added for keyed messages.
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
    ChannelMessage channelMessage = sendMessage.getChannelMessage();
    if (channelMessage.isComplete()) {
      sendMessage.setSendState(OutMessage.SendState.SERIALIZED);
      return sendMessage;
    }

    // we set the serialize state here, this will be used by subsequent calls
    // to keep track of the serialization communicationProgress of this message
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
        channelMessage.setPartial(true);
      }

      if (sendMessage.serializedState() == OutMessage.SendState.HEADER_BUILT
          || sendMessage.serializedState() == OutMessage.SendState.BODY_BUILT
          || sendMessage.serializedState() == OutMessage.SendState.PARTIALLY_SERIALIZED) {
        if ((sendMessage.getFlags() & MessageFlags.END) == MessageFlags.END) {
          sendMessage.setSendState(OutMessage.SendState.SERIALIZED);
          sendMessage.getSerializationState().setTotalBytes(0);
        } else {
          // first we need to serialize the body if needed
          if (sendMessage.serializedState() == OutMessage.SendState.PARTIALLY_SERIALIZED
              && sendMessage.getSerializationState().getData() == null) {
            if (!channelMessage.isHeaderSent()) {
              buildHeader(buffer, sendMessage);
              channelMessage.setPartial(true);
            }
          }
          serializeBody(message, sendMessage, buffer);
        }
      }

      // okay we are adding this buffer
      channelMessage.addBuffer(buffer);
      if (sendMessage.serializedState() == OutMessage.SendState.SERIALIZED) {
        SerializeState state = sendMessage.getSerializationState();

        int totalBytes = state.getTotalBytes();
        channelMessage.getBuffers().get(0).getByteBuffer().putInt(HEADER_SIZE - Integer.BYTES,
            totalBytes);

        MessageHeader.Builder builder = MessageHeader.newBuilder(sendMessage.getSource(),
            sendMessage.getEdge(), totalBytes);
        builder.destination(sendMessage.getPath());
        channelMessage.setHeader(builder.build());
        state.setTotalBytes(0);

        // mark the original message as complete
        channelMessage.setComplete(true);
      } else if (sendMessage.serializedState() == OutMessage.SendState.PARTIALLY_SERIALIZED) {
        SerializeState state = sendMessage.getSerializationState();

        if (channelMessage.isPartial()) {
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
        } else {
          int totalBytes = state.getTotalBytes();
          //Need to calculate the true total bites since a part of the message may come separately
          channelMessage.getBuffers().get(0).getByteBuffer().putInt(HEADER_SIZE - Integer.BYTES,
              totalBytes);

          MessageHeader.Builder builder = MessageHeader.newBuilder(sendMessage.getSource(),
              sendMessage.getEdge(), totalBytes);
          builder.destination(sendMessage.getPath());
          channelMessage.setHeader(builder.build());
        }
        state.setTotalBytes(0);

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
    // the destination id
    byteBuffer.putInt(sendMessage.getPath());
    // we add 0 for length now and later change it
    byteBuffer.putInt(0);
    // at this point we haven't put the length and we will do it at the serialization
    sendMessage.setWrittenHeaderSize(HEADER_SIZE);
    // lets set the size for 16 for now
    buffer.setSize(HEADER_SIZE);
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
    List objectList = (List) payload;
    SerializeState state = sendMessage.getSerializationState();

    int startIndex = state.getCurrentObject();
    //Keeps track of the number of objects that are currently in the targetBuffer
    int countInBuffer = 0;

    // we assume remaining = capacity of the targetBuffer as we always get a fresh targetBuffer her
    int remaining = targetBuffer.getByteBuffer().remaining();
    // we cannot use this targetBuffer as we cannot put the sub header
    if (remaining <= MAX_SUB_MESSAGE_HEADER_SPACE) {
      throw new RuntimeException("This targetBuffer is too small to fit a message: " + remaining);
    }

    // we will copy until we have space left or we are have serialized all the objects
    for (int i = startIndex; i < objectList.size(); i++) {
      Object o = objectList.get(i);
      if (o instanceof ChannelMessage) {
        ChannelMessage channelMessage = (ChannelMessage) o;
        boolean complete = serializeBufferedMessage(channelMessage, state,
            targetBuffer, countInBuffer);
        // we copied this completely
        if (complete) {
          sendMessage.getChannelMessage().setPartial(false);
          state.setCurretHeaderLength(state.getTotalBytes());
          state.setCurrentObject(i + 1);
          countInBuffer++;
        } else {
          break;
        }
      } else {
        boolean complete = serializeMessage(o, sendMessage, targetBuffer, countInBuffer);
        if (complete) {
          sendMessage.getChannelMessage().setPartial(false);
          state.setCurretHeaderLength(state.getTotalBytes());
          state.setCurrentObject(i + 1);
          countInBuffer++;
        } else {
          break;
        }
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
    if (state.getCurrentObject() == objectList.size()) {
      sendMessage.setSendState(OutMessage.SendState.SERIALIZED);
    } else {
      sendMessage.setSendState(OutMessage.SendState.PARTIALLY_SERIALIZED);
    }
  }

  /**
   * Serialized the message into the buffer
   *
   * @return true if the message is completely written
   */
  /**
   * Serializes the message given in the payload. This method will call helper methods to perform
   * the actual serialization based on the type of message.
   *
   * @param payload the message that needs to be serialized
   * @param sendMessage the send message object that contains all the metadata
   * @param targetBuffer data buffer that the data is copied to
   * @param countInBuffer the number of objects that are currently in the targetBuffer.
   * This is important when a large message that cannot be fit into a single fixed buffer needs to
   * be processed. Such messages are only copied to the current buffer if there are no other
   * messages in the current targetBuffer. If there is, the system will stop any further copying to
   * the current target buffer and request a new buffer to start copying the next object in the
   * multi message.
   * @return true if the message was successfully copied into the targetBuffer, false otherwise.
   */
  private boolean serializeMessage(Object payload, OutMessage sendMessage,
                                   DataBuffer targetBuffer, int countInBuffer) {
    MessageType type = sendMessage.getChannelMessage().getType();
    if (!keyed) {
      return serializeData(payload,
          sendMessage.getSerializationState(), targetBuffer, type, countInBuffer);
    } else {
      KeyedContent kc = (KeyedContent) payload;
      return serializeKeyedData(kc.getValue(), kc.getKey(), sendMessage.getSerializationState(),
          targetBuffer, kc.getContentType(), kc.getKeyType(), countInBuffer);
    }
  }

  /**
   * Serializes messages that are sent as {@link ChannelMessage}'s. This is useful when the current
   * node is forwarding an incoming message from another to its target destination. Since the
   * data is already in buffers it is much faster than building the message from scratch.
   *
   * @param message channel message that needs to be built
   * @param state state object that keeps state information
   * @param targetBuffer data buffer that the data is copied to
   * @param countInBuffer the number of objects that are currently in the targetBuffer.
   * This is important when a large message that cannot be fit into a single fixed buffer needs to
   * be processed. Such messages are only copied to the current buffer if there are no other
   * messages in the current targetBuffer. If there is, the system will stop any further copying to
   * the current target buffer and request a new buffer to start copying the next object in the
   * multi message.
   * @return true if the message was successfully copied into the targetBuffer, false otherwise.
   */
  private boolean serializeBufferedMessage(ChannelMessage message, SerializeState state,
                                           DataBuffer targetBuffer, int countInBuffer) {
    ByteBuffer targetByteBuffer = targetBuffer.getByteBuffer();
    byte[] tempBytes = new byte[targetBuffer.getCapacity()];
    // the target remaining space left
    int targetRemainingSpace = targetByteBuffer.remaining();

    //If we cannot fit the whole message in the current buffer return false
    if (message.getHeader() != null) {
      state.setCurretHeaderLength(message.getHeader().getLength() + message.getHeaderSize());
      if (countInBuffer > 0 && targetRemainingSpace < message.getHeader().getLength()) {
        return false;
      }
    } else {
      throw new RuntimeException(executor + " The header in the message must be built");
    }
    // the current buffer number
    int currentSourceBuffer = state.getBufferNo();
    // bytes already copied from this buffer
    int bytesCopiedFromSource = state.getBytesCopied();
    int canCopy = 0;
    int needsCopy = 0;
    List<DataBuffer> buffers = message.getBuffers();
    DataBuffer currentDataBuffer = null;
    int totalBytes = state.getTotalBytes();
    while (targetRemainingSpace > 0 && currentSourceBuffer < buffers.size()) {
      currentDataBuffer = buffers.get(currentSourceBuffer);
      ByteBuffer currentSourceByteBuffer = currentDataBuffer.getByteBuffer();
      // 0th buffer has the header
      if (currentSourceBuffer == 0 && bytesCopiedFromSource == 0) {
        // we add 16 because,
        bytesCopiedFromSource += HEADER_SIZE;
      }
      needsCopy = currentDataBuffer.getSize() - bytesCopiedFromSource;
      currentSourceByteBuffer.position(bytesCopiedFromSource);

      canCopy = needsCopy > targetRemainingSpace ? targetRemainingSpace : needsCopy;
      currentSourceByteBuffer.get(tempBytes, 0, canCopy);
      // todo check this method
      targetByteBuffer.put(tempBytes, 0, canCopy);
      totalBytes += canCopy;
      targetRemainingSpace -= canCopy;
      bytesCopiedFromSource += canCopy;

      // the target buffer is full, we need to return
      if (targetRemainingSpace < NORMAL_SUB_MESSAGE_HEADER_SIZE) {
        // now check weather we can move to the next source buffer
        if (canCopy == needsCopy) {
          currentSourceBuffer++;
          bytesCopiedFromSource = 0;
        }
        break;
      }

      // if there is space we will copy everything from the source buffer and we need to move
      // to next
      currentSourceBuffer++;
      bytesCopiedFromSource = 0;
    }

    // set the data size of the target buffer
    targetBuffer.setSize(targetByteBuffer.position());
    state.setTotalBytes(totalBytes);
    if (currentSourceBuffer == buffers.size() && currentDataBuffer != null) {
      state.setBufferNo(0);
      state.setBytesCopied(0);
      message.release();
      return true;
    } else {
      state.setBufferNo(currentSourceBuffer);
      state.setBytesCopied(bytesCopiedFromSource);
      return false;
    }
  }

  /**
   * Builds the sub message header which is used in multi messages to identify the lengths of each
   * sub message. The structure of the sub message header is |length + (key length)|. The key length
   * is added for keyed messages
   */
  private boolean buildSubMessageHeader(DataBuffer buffer, int length) {
    ByteBuffer byteBuffer = buffer.getByteBuffer();
    if (byteBuffer.remaining() < NORMAL_SUB_MESSAGE_HEADER_SIZE) {
      return false;
    }
    byteBuffer.putInt(length);
    return true;
  }

  /**
   * Helper method that builds the body of the message for regular messages.
   *
   * @param payload the message that needs to be built
   * @param state the state object of the message
   * @param targetBuffer the data targetBuffer to which the built message needs to be copied
   * @param messageType the type of the message data
   * @param countInBuffer countInBuffer the number of objects that are currently in the targetBuffer
   * This is important when a large message that cannot be fit into a single fixed buffer needs to
   * be processed. Such messages are only copied to the current buffer if there are no other
   * messages in the current targetBuffer. If there is, the system will stop any further copying to
   * the current target buffer and request a new buffer to start copying the next object in the
   * multi message.
   * @return true if the message was successfully copied into the targetBuffer, false otherwise.
   */
  private boolean serializeData(Object payload, SerializeState state, DataBuffer targetBuffer,
                                MessageType messageType, int countInBuffer) {
    ByteBuffer byteBuffer = targetBuffer.getByteBuffer();
    // okay we need to serialize the header
    if (state.getPart() == SerializeState.Part.INIT) {
      // okay we need to serialize the data
      int dataLength = DataSerializer.serializeData(payload, messageType, state, serializer);
      int remaining = targetBuffer.getByteBuffer().remaining();
      state.setCurretHeaderLength(dataLength);
      //Check if we can fit this message into the current buffer
      //If not we will return so that this message is added to the next buffer
      //However if this is the first message and we cannot fit it into the current buffer we will
      //break the message down into couple of buffers
      if (countInBuffer > 0 && remaining < dataLength + MAX_SUB_MESSAGE_HEADER_SPACE) {
        LOG.fine("Cannot insert into current buffer this message will be entered"
            + " into the next buffer");
        return false;
      }

      if (!buildSubMessageHeader(targetBuffer, dataLength)) {
        LOG.warning("We should always be able to build the header in the current buffer");
        return false;
      }
      // add the header bytes to the total bytes
      state.addTotalBytes(NORMAL_SUB_MESSAGE_HEADER_SIZE);
      state.setCurretHeaderLength(dataLength + NORMAL_SUB_MESSAGE_HEADER_SIZE);
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
   * Helper method that builds the body of the message for keyed messages.
   *
   * @param payload the message that needs to be built
   * @param key the key associated with the message
   * @param state the state object of the message
   * @param targetBuffer the data targetBuffer to which the built message needs to be copied
   * @param messageType the type of the message data
   * @param keyType the type of the message key
   * @param countInBuffer countInBuffer the number of objects that are currently in the targetBuffer
   * This is important when a large message that cannot be fit into a single fixed buffer needs to
   * be processed. Such messages are only copied to the current buffer if there are no other
   * messages in the current targetBuffer. If there is, the system will stop any further copying to
   * the current target buffer and request a new buffer to start copying the next object in the
   * multi message.
   * @return true if the message was successfully copied into the targetBuffer, false otherwise.
   */
  private boolean serializeKeyedData(Object payload, Object key, SerializeState state,
                                     DataBuffer targetBuffer, MessageType messageType,
                                     MessageType keyType, int countInBuffer) {
    ByteBuffer byteBuffer = targetBuffer.getByteBuffer();
    // okay we need to serialize the header
    if (state.getPart() == SerializeState.Part.INIT) {
      int keyLength = KeySerializer.serializeKey(key,
          keyType, state, serializer);
      // okay we need to serialize the data
      int dataLength = DataSerializer.serializeData(payload,
          messageType, state, serializer);
      int remaining = targetBuffer.getByteBuffer().remaining();

      //Check if we can fit this message into the current buffer
      //If not we will return so that this message is added to the next buffer
      //However if this is the first message and we cannot fit it into the current buffer we will
      //break the message down into couple of buffers
      if (countInBuffer > 0 && remaining < dataLength + MAX_SUB_MESSAGE_HEADER_SPACE) {
        LOG.fine("Cannot insert into current buffer this message will be entered"
            + " into the next buffer");
        return false;
      }
//      LOG.info(String.format("%d serialize data length: %d pos %d",
//          executor, dataLength, byteBuffer.position()));
      // at this point we know the length of the data
      if (!buildSubMessageHeader(targetBuffer, dataLength + keyLength)) {
        LOG.warning("We should always be able to build the header in the current buffer");
        return false;
      }
      // add the header bytes to the total bytes
      state.setTotalBytes(state.getTotalBytes() + NORMAL_SUB_MESSAGE_HEADER_SIZE);
      state.setCurretHeaderLength(keyLength + dataLength + NORMAL_SUB_MESSAGE_HEADER_SIZE);
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
}
