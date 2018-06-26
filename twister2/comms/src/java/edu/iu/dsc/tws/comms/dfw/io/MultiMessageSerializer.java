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

  @Override
  public Object build(Object message, Object partialBuildObject) {
    int noOfMessages = 1;
    if (message instanceof List) {
      noOfMessages = ((List) message).size();
    }
    OutMessage sendMessage = (OutMessage) partialBuildObject;

    // we got an already serialized message, lets just return it
    ChannelMessage channelMessage = sendMessage.getMPIMessage();
    if (channelMessage.isComplete()) {
      sendMessage.setSendState(OutMessage.SendState.SERIALIZED);
      return sendMessage;
    }

    // we set the serialize state here, this will be used by subsequent calls
    // to keep track of the serialization progress of this message
    if (sendMessage.getSerializationState() == null) {
      sendMessage.setSerializationState(new SerializeState());
    }

    while (sendBuffers.size() > 0 && sendMessage.serializedState()
        != OutMessage.SendState.SERIALIZED) {
      DataBuffer buffer = sendBuffers.poll();

      if (sendMessage.serializedState() == OutMessage.SendState.INIT
          || sendMessage.serializedState() == OutMessage.SendState.SENT_INTERNALLY) {
        // build the header
        buildHeader(buffer, sendMessage, noOfMessages);
        sendMessage.setSendState(OutMessage.SendState.HEADER_BUILT);
      }

      if (sendMessage.serializedState() == OutMessage.SendState.HEADER_BUILT
          || sendMessage.serializedState() == OutMessage.SendState.BODY_BUILT) {
        if ((sendMessage.getFlags() & MessageFlags.EMPTY) == MessageFlags.EMPTY) {
          sendMessage.setSendState(OutMessage.SendState.SERIALIZED);
          sendMessage.getSerializationState().setTotalBytes(0);
        } else {
          // first we need to serialize the body if needed
          serializeBody(message, sendMessage, buffer);
        }
      }

      // okay we are adding this buffer
      channelMessage.addBuffer(buffer);
      if (sendMessage.serializedState() == OutMessage.SendState.SERIALIZED) {
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
      }
    }
    return sendMessage;
  }

  private void buildHeader(DataBuffer buffer, OutMessage sendMessage, int noOfMessage) {
    if (buffer.getCapacity() < HEADER_SIZE) {
      throw new RuntimeException("The buffers should be able to hold the complete header");
    }
    ByteBuffer byteBuffer = buffer.getByteBuffer();
    // now lets put the content of header in
    byteBuffer.putInt(sendMessage.getSource());
    // the path we are on, if not grouped it will be 0 and ignored
    byteBuffer.putInt(sendMessage.getFlags());
    // the destination id
    byteBuffer.putInt(sendMessage.getDestintationIdentifier());
    // we add 0 for length now and later change it
    byteBuffer.putInt(noOfMessage);
    // at this point we haven't put the length and we will do it at the serialization
    sendMessage.setWrittenHeaderSize(HEADER_SIZE);
    // lets set the size for 16 for now
    buffer.setSize(HEADER_SIZE);
  }

  /**
   * Serialized the message into the buffer
   *
   * @return true if the message is completely written
   */
  private boolean serializeMessage(Object payload,
                                   OutMessage sendMessage, DataBuffer buffer) {
    MessageType type = sendMessage.getMPIMessage().getType();
    if (!keyed) {
      return serializeData(payload,
          sendMessage.getSerializationState(), buffer, type);
    } else {
      KeyedContent kc = (KeyedContent) payload;
      return serializeKeyedData(kc.getValue(), kc.getKey(),
          sendMessage.getSerializationState(), buffer, kc.getContentType(), kc.getKeyType());
    }
  }

  @SuppressWarnings("rawtypes")
  private void serializeBody(Object object, OutMessage sendMessage, DataBuffer buffer) {
    List objectList = (List) object;
    SerializeState state = sendMessage.getSerializationState();

    int startIndex = state.getCurrentObject();

    // we assume remaining = capacity of the buffer as we always get a fresh buffer her
    int remaining = buffer.getByteBuffer().remaining();
    // we cannot use this buffer as we cannot put the sub header
    if (remaining <= MAX_SUB_MESSAGE_HEADER_SPACE) {
      throw new RuntimeException("This buffer is too small to fit a message: " + remaining);
    }

    // we will copy until we have space left or we are have serialized all the objects
    for (int i = startIndex; i < objectList.size(); i++) {
      Object o = objectList.get(i);
      if (o instanceof ChannelMessage) {
        ChannelMessage channelMessage = (ChannelMessage) o;
        boolean complete = serializeBufferedMessage(channelMessage, state, buffer);
        // we copied this completely
        if (complete) {
          state.setCurrentObject(i + 1);
        } else {
          break;
        }
      } else {
        boolean complete = serializeMessage(o, sendMessage, buffer);
        if (complete) {
          state.setCurrentObject(i + 1);
        } else {
          break;
        }
      }

      // check how much space left in this buffer
      remaining = buffer.getByteBuffer().remaining();
      // if we have less than this amount of space, that means we may not be able to put the next
      // header in a contigous space, so we cannot use this buffer anymore
      if (!(remaining > MAX_SUB_MESSAGE_HEADER_SPACE
          && state.getCurrentObject() < objectList.size())) {
        break;
      }
    }
    if (state.getCurrentObject() == objectList.size()) {
      sendMessage.setSendState(OutMessage.SendState.SERIALIZED);
    }
  }

  /**
   * Serialize a message in buffers.
   *
   * @return the number of complete messages written
   */
  private boolean serializeBufferedMessage(ChannelMessage message, SerializeState state,
                                           DataBuffer targetBuffer) {
    ByteBuffer targetByteBuffer = targetBuffer.getByteBuffer();
    byte[] tempBytes = new byte[targetBuffer.getCapacity()];
    // the target remaining space left
    int targetRemainingSpace = targetByteBuffer.remaining();
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

  private boolean buildSubMessageHeader(DataBuffer buffer, int length) {
    ByteBuffer byteBuffer = buffer.getByteBuffer();
    if (byteBuffer.remaining() < 4) {
      return false;
    }
    byteBuffer.putInt(length);
    return true;
  }

  /**
   * Serializes a java object using kryo serialization
   */
  private boolean serializeData(Object content, SerializeState state,
                                DataBuffer targetBuffer, MessageType messageType) {
    ByteBuffer byteBuffer = targetBuffer.getByteBuffer();
    // okay we need to serialize the header
    if (state.getPart() == SerializeState.Part.INIT) {
      // okay we need to serialize the data
      int dataLength = DataSerializer.serializeData(content, messageType, state, serializer);

      if (!buildSubMessageHeader(targetBuffer, dataLength)) {
        LOG.warning("We should always be able to build the header in the current buffer");
        return false;
      }
      // add the header bytes to the total bytes
      state.addTotalBytes(NORMAL_SUB_MESSAGE_HEADER_SIZE);
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


  private boolean serializeKeyedData(Object content, Object key, SerializeState state,
                                     DataBuffer targetBuffer,
                                     MessageType contentType, MessageType keyType) {
    ByteBuffer byteBuffer = targetBuffer.getByteBuffer();
    // okay we need to serialize the header
    if (state.getPart() == SerializeState.Part.INIT) {
      int keyLength = KeySerializer.serializeKey(key,
          keyType, state, serializer);
      // okay we need to serialize the data
      int dataLength = DataSerializer.serializeData(content,
          contentType, state, serializer);
//      LOG.info(String.format("%d serialize data length: %d pos %d",
//          executor, dataLength, byteBuffer.position()));
      // at this point we know the length of the data
      if (!buildSubMessageHeader(targetBuffer, dataLength + keyLength)) {
        LOG.warning("We should always be able to build the header in the current buffer");
        return false;
      }
      // add the header bytes to the total bytes
      state.setTotalBytes(state.getTotalBytes() + NORMAL_SUB_MESSAGE_HEADER_SIZE);
//      LOG.info(String.format("%d pos after header %d",
//          executor, byteBuffer.position()));
//      LOG.info(String.format("%d total after header %d",
//          executor, state.getTotalBytes()));
    }

    if (state.getPart() == SerializeState.Part.INIT
        || state.getPart() == SerializeState.Part.HEADER) {
      boolean complete = KeySerializer.copyKeyToBuffer(key,
          keyType, targetBuffer.getByteBuffer(), state, serializer);
//      LOG.info(String.format("%d pos after key copy %d",
//          executor, byteBuffer.position()));
//      LOG.info(String.format("%d total after key %d",
//          executor, state.getTotalBytes()));
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
//    LOG.info(String.format("%d pos after data %d",
//        executor, byteBuffer.position()));
    // now set the size of the buffer
    targetBuffer.setSize(byteBuffer.position());

    // okay we are done with the message
    if (completed) {
      // add the key size at the end to total size
//      LOG.info(String.format("%d total after complete %d",
//          executor, state.getTotalBytes()));
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
