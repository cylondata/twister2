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
import java.util.List;
import java.util.Queue;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.MessageHeader;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.mpi.MPIBuffer;
import edu.iu.dsc.tws.comms.mpi.MPIMessage;
import edu.iu.dsc.tws.comms.mpi.MPISendMessage;
import edu.iu.dsc.tws.comms.mpi.io.types.ObjectSerializer;
import edu.iu.dsc.tws.comms.utils.KryoSerializer;

public class MPIMultiMessageSerializer implements MessageSerializer {
  private static final Logger LOG = Logger.getLogger(MPIMultiMessageSerializer.class.getName());

  private Queue<MPIBuffer> sendBuffers;
  private KryoSerializer serializer;
  private ObjectSerializer objectSerializer;
  private int executor;

  private static final int HEADER_SIZE = 16;
  // we need to put the message length and key length if keyed message
  private static final int MAX_SUB_MESSAGE_HEADER_SPACE = 4 + 4;
  // for s normal message we only put the length
  private static final int NORMAL_SUB_MESSAGE_HEADER_SIZE = 4;

  public MPIMultiMessageSerializer(KryoSerializer kryoSerializer, int exec) {
    this.serializer = kryoSerializer;
    this.objectSerializer = new ObjectSerializer(serializer);
    this.executor = exec;
  }

  @Override
  public void init(Config cfg, Queue<MPIBuffer> buffers) {
    this.sendBuffers = buffers;
  }

  @Override
  public Object build(Object message, Object partialBuildObject) {
    int noOfMessages = 1;
    if (message instanceof List) {
      noOfMessages = ((List) message).size();
    }
    MPISendMessage sendMessage = (MPISendMessage) partialBuildObject;

    // we got an already serialized message, lets just return it
    MPIMessage mpiMessage = sendMessage.getMPIMessage();
    if (mpiMessage.isComplete()) {
      sendMessage.setSendState(MPISendMessage.SendState.SERIALIZED);
      return sendMessage;
    }

    // we set the serialize state here, this will be used by subsequent calls
    // to keep track of the serialization progress of this message
    sendMessage.setSerializationState(new SerializeState());

    while (sendBuffers.size() > 0 && sendMessage.serializedState()
        != MPISendMessage.SendState.SERIALIZED) {
      MPIBuffer buffer = sendBuffers.poll();

      if (sendMessage.serializedState() == MPISendMessage.SendState.INIT
          || sendMessage.serializedState() == MPISendMessage.SendState.SENT_INTERNALLY) {
        // build the header
        buildHeader(buffer, sendMessage, noOfMessages);
        sendMessage.setSendState(MPISendMessage.SendState.HEADER_BUILT);
      }

      if (sendMessage.serializedState() == MPISendMessage.SendState.HEADER_BUILT
          || sendMessage.serializedState() == MPISendMessage.SendState.BODY_BUILT) {
        // first we need to serialize the body if needed
        serializeBody(message, sendMessage, buffer);
      }

      // okay we are adding this buffer
      mpiMessage.addBuffer(buffer);
      if (sendMessage.serializedState() == MPISendMessage.SendState.SERIALIZED) {
        SerializeState state = (SerializeState) sendMessage.getSerializationState();
        int totalBytes = state.getTotalBytes();
        mpiMessage.getBuffers().get(0).getByteBuffer().putInt(12, totalBytes);

        MessageHeader.Builder builder = MessageHeader.newBuilder(sendMessage.getSource(),
            sendMessage.getEdge(), totalBytes);
        builder.destination(sendMessage.getDestintationIdentifier());
        sendMessage.getMPIMessage().setHeader(builder.build());
        state.setTotalBytes(0);

        // mark the original message as complete
        mpiMessage.setComplete(true);
      }
    }
    return sendMessage;
  }

  private void buildHeader(MPIBuffer buffer, MPISendMessage sendMessage, int noOfMessage) {
    if (buffer.getCapacity() < HEADER_SIZE) {
      throw new RuntimeException("The buffers should be able to hold the complete header");
    }
    ByteBuffer byteBuffer = buffer.getByteBuffer();
//    LOG.info(String.format("%d adding header pos: %d", executor, byteBuffer.position()));
    // now lets put the content of header in
    byteBuffer.putInt(sendMessage.getSource());
    // the path we are on, if not grouped it will be 0 and ignored
    byteBuffer.putInt(sendMessage.getFlags());
    byteBuffer.putInt(sendMessage.getDestintationIdentifier());
    // we add 0 for now and late change it
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
                                   MPISendMessage sendMessage, MPIBuffer buffer) {
    MessageType type = sendMessage.getMPIMessage().getType();
    switch (type) {
      case INTEGER:
        break;
      case LONG:
        break;
      case DOUBLE:
        break;
      case OBJECT:
        return serializeObject(payload,
            (SerializeState) sendMessage.getSerializationState(), buffer);
      case BYTE:
        break;
      case STRING:
        break;
      case BUFFER:
        break;
      case KEYED:
        return serializeKeyedObject((KeyedContent) payload,
            (SerializeState) sendMessage.getSerializationState(), buffer);
      default:
        break;
    }
    return false;
  }

  @SuppressWarnings("rawtypes")
  private void serializeBody(Object object, MPISendMessage sendMessage, MPIBuffer buffer) {
    List objectList = (List) object;
    SerializeState state =
        (SerializeState) sendMessage.getSerializationState();

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
      if (o instanceof MPIMessage) {
        MPIMessage mpiMessage = (MPIMessage) o;
        boolean complete = serializeBufferedMessage(mpiMessage, state, buffer);
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
      sendMessage.setSendState(MPISendMessage.SendState.SERIALIZED);
    }
  }

  /**
   * Serialize a message in buffers.
   *
   * @return the number of complete messages written
   */
  private boolean serializeBufferedMessage(MPIMessage message, SerializeState state,
                                           MPIBuffer targetBuffer) {
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
    List<MPIBuffer> buffers = message.getBuffers();
    MPIBuffer currentMPIBuffer = null;
    int totalBytes = state.getTotalBytes();
    while (targetRemainingSpace > 0 && currentSourceBuffer < buffers.size()) {
      currentMPIBuffer = buffers.get(currentSourceBuffer);
      ByteBuffer currentSourceByteBuffer = currentMPIBuffer.getByteBuffer();
      // 0th buffer has the header
      if (currentSourceBuffer == 0 && bytesCopiedFromSource == 0) {
        // we add 16 because,
        bytesCopiedFromSource += HEADER_SIZE;
      }
      needsCopy = currentMPIBuffer.getSize() - bytesCopiedFromSource;
//      LOG.info(String.format("%d position %d %d", executor, bytesCopiedFromSource,
//          currentSourceByteBuffer.limit()));
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
    if (currentSourceBuffer == buffers.size() && currentMPIBuffer != null) {
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

  private void buildSubMessageHeader(MPIBuffer buffer, int length) {
    ByteBuffer byteBuffer = buffer.getByteBuffer();
//    LOG.info(String.format("%d adding sub-header pos: %d", executor, byteBuffer.position()));
    byteBuffer.putInt(length);
  }

  /**
   * Serializes a java object using kryo serialization
   */
  private boolean serializeObject(Object object, SerializeState state,
                                 MPIBuffer targetBuffer) {
    byte[] data;
    int dataPosition = 0;
    ByteBuffer byteBuffer = targetBuffer.getByteBuffer();
    int totalBytes = state.getTotalBytes();
    if (state.getBytesCopied() == 0) {
      // okay we need to serialize the data
      data = serializer.serialize(object);

      // at this point we know the length of the data
      buildSubMessageHeader(targetBuffer, data.length);
      // add the header bytes to the total bytes
      totalBytes += NORMAL_SUB_MESSAGE_HEADER_SIZE;
      state.setData(data);
    } else {
      data = state.getData();
      dataPosition = state.getBytesCopied();
    }

    int remainingToCopy = data.length - dataPosition;
    // check how much space we have
    int bufferSpace = byteBuffer.capacity() - byteBuffer.position();

    int copyBytes = remainingToCopy > bufferSpace ? bufferSpace : remainingToCopy;
    // check how much space left in the buffer
    byteBuffer.put(data, dataPosition, copyBytes);
    state.setBytesCopied(dataPosition + copyBytes);
    state.setTotalBytes(totalBytes + copyBytes);
    // now set the size of the buffer
    targetBuffer.setSize(byteBuffer.position());

    // okay we are done with the message
    if (copyBytes == remainingToCopy) {
      state.setBytesCopied(0);
      state.setBufferNo(0);
      state.setData(null);
      return true;
    } else {
      return false;
    }
  }

  private boolean serializeKeyedObject(KeyedContent content, SerializeState state,
                                      MPIBuffer targetBuffer) {
    byte[] data;
    int dataPosition = 0;
    ByteBuffer byteBuffer = targetBuffer.getByteBuffer();
    int totalBytes = state.getTotalBytes();
    // okay we need to serialize the header
    if (state.getPart() == SerializeState.Part.INIT) {
      int keyLength = serializeKey(content, targetBuffer.getByteBuffer(), state);
      // okay we need to serialize the data
      data = serializer.serialize(content.getObject());
      // at this point we know the length of the data
      buildSubMessageHeader(targetBuffer, data.length + keyLength);
      // add the header bytes to the total bytes
      totalBytes += NORMAL_SUB_MESSAGE_HEADER_SIZE;
      state.setData(data);

      copyKeyToBuffer(content, targetBuffer.getByteBuffer(), state);
    }

    if (state.getPart() == SerializeState.Part.HEADER) {
      copyKeyToBuffer(content, targetBuffer.getByteBuffer(), state);
    }

    // now we can serialize the body
    if (state.getPart() != SerializeState.Part.BODY) {
      return false;
    }

    data = state.getData();
    dataPosition = state.getBytesCopied();

    int remainingToCopy = data.length - dataPosition;
    // check how much space we have
    int bufferSpace = byteBuffer.capacity() - byteBuffer.position();

    int copyBytes = remainingToCopy > bufferSpace ? bufferSpace : remainingToCopy;
//    LOG.info(String.format("%d copy keyed pos: %d", executor, byteBuffer.position()));
    // check how much space left in the buffer
    byteBuffer.put(data, dataPosition, copyBytes);
    state.setBytesCopied(dataPosition + copyBytes);
    state.setTotalBytes(totalBytes + copyBytes);
    // now set the size of the buffer
    targetBuffer.setSize(byteBuffer.position());

    // okay we are done with the message
    if (copyBytes == remainingToCopy) {
      // add the key size at the end to total size
      state.setTotalBytes(state.getTotalBytes() + state.getKeySize());
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

  private int serializeKey(KeyedContent content, ByteBuffer targetBuffer, SerializeState state) {
    switch (content.getKeyType()) {
      case INTEGER:
        return 4;
      case SHORT:
        return 2;
      case LONG:
        return 8;
      case DOUBLE:
        return 8;
      case OBJECT:
        if (state.getKey() == null) {
          byte[] serialize = serializer.serialize(content.getSource());
          state.setKey(serialize);
        }
        return state.getKey().length;
      case BYTE:
        if (state.getKey() == null) {
          state.setKey((byte[]) content.getSource());
        }
        return state.getKey().length;
      case STRING:
        if (state.getKey() == null) {
          state.setKey(((String) content.getSource()).getBytes());
        }
        return state.getKey().length;
      default:
        break;
    }
    return 0;
  }

  private void copyKeyToBuffer(KeyedContent content,
                               ByteBuffer targetBuffer, SerializeState state) {
//    LOG.info(String.format("%d copy key: %d", executor, targetBuffer.position()));

    switch (content.getKeyType()) {
      case INTEGER:
        if (targetBuffer.remaining() > 4) {
          targetBuffer.putInt((Integer) content.getSource());
          state.setPart(SerializeState.Part.BODY);
          state.setTotalBytes(state.getTotalBytes() + 4);
          state.setKeySize(4);
        }
        break;
      case SHORT:
        if (targetBuffer.remaining() > 2) {
          targetBuffer.putShort((short) content.getSource());
          state.setPart(SerializeState.Part.BODY);
          state.setTotalBytes(state.getTotalBytes() + 2);
          state.setKeySize(2);
        }
        break;
      case LONG:
        if (targetBuffer.remaining() > 8) {
          targetBuffer.putLong((Long) content.getSource());
          state.setPart(SerializeState.Part.BODY);
          state.setTotalBytes(state.getTotalBytes() + 8);
          state.setKeySize(8);
        }
        break;
      case DOUBLE:
        if (targetBuffer.remaining() > 8) {
          targetBuffer.putDouble((Double) content.getSource());
          state.setPart(SerializeState.Part.BODY);
          state.setTotalBytes(state.getTotalBytes() + 8);
          state.setKeySize(8);
        }
        break;
      case OBJECT:
        if (state.getKey() == null) {
          byte[] serialize = serializer.serialize(content.getSource());
          state.setKey(serialize);
        }
        copyKeyBytes(targetBuffer, state);
        break;
      case BYTE:
        if (state.getKey() == null) {
          state.setKey((byte[]) content.getSource());
        }
        copyKeyBytes(targetBuffer, state);
        break;
      case STRING:
        if (state.getKey() == null) {
          state.setKey(((String) content.getSource()).getBytes());
        }
        copyKeyBytes(targetBuffer, state);
        break;
      default:
        break;
    }
  }

  private int copyKeyBytes(ByteBuffer targetBuffer, SerializeState state) {
    int totalBytes = state.getTotalBytes();
    int remainingCapacity = targetBuffer.remaining();
    int bytesCopied = state.getBytesCopied();

    byte[] key = state.getKey();
    if (bytesCopied == 0 && remainingCapacity > 4) {
      targetBuffer.putInt(key.length);
      totalBytes += 4;
    } else {
      return 0;
    }

    int remainingToCopy = key.length - bytesCopied;
    int canCopy = remainingCapacity > remainingToCopy ? remainingToCopy : remainingCapacity;
    // copy
    targetBuffer.put(key, bytesCopied, canCopy);
    totalBytes += canCopy;
    // we set the tolal bytes copied so far
    state.setTotalBytes(totalBytes);
    // we copied everything
    if (canCopy == remainingToCopy) {
      state.setKey(null);
      state.setBytesCopied(0);
      state.setPart(SerializeState.Part.BODY);
    } else {
      state.setBytesCopied(canCopy + bytesCopied);
    }
    // we will use this size later
    state.setKeySize(key.length + 4);
    return key.length;
  }
}
