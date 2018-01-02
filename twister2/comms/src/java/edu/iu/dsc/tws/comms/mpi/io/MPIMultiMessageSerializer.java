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
import edu.iu.dsc.tws.comms.api.MessageSerializer;
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

  public MPIMultiMessageSerializer(Queue<MPIBuffer> buffers,
                                   KryoSerializer kryoSerializer, int exec) {
    this.sendBuffers = buffers;
    this.serializer = kryoSerializer;
    this.objectSerializer = new ObjectSerializer(serializer);
    this.executor = exec;
  }

  @Override
  public void init(Config cfg) {

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
        // build the body
        // first we need to serialize the body if needed
        serializeBody(message, sendMessage, buffer);
      }

      // okay we are adding this buffer
      mpiMessage.addBuffer(buffer);
      if (sendMessage.serializedState() == MPISendMessage.SendState.SERIALIZED) {
        SerializeState state = (SerializeState) sendMessage.getSerializationState();
        int totalBytes = state.getTotalBytes();
        mpiMessage.getBuffers().get(0).getByteBuffer().putInt(
            12, totalBytes);

        MessageHeader.Builder builder = MessageHeader.newBuilder(sendMessage.getSource(),
            sendMessage.getEdge(), totalBytes);
        builder.destination(sendMessage.getDestintationIdentifier());
        sendMessage.getMPIMessage().setHeader(builder.build());
//        if (message instanceof List) {
//          LOG.info(String.format("%d serialize length %d size %d",
//              executor, totalBytes, ((List) message).size()));
//        }
        state.setTotalBytes(0);

        // mark the original message as complete
        mpiMessage.setComplete(true);
      } else {
        LOG.info("Message NOT FULLY serialized");
      }
    }
    return sendMessage;
  }

  private void buildHeader(MPIBuffer buffer, MPISendMessage sendMessage, int noOfMessage) {
    if (buffer.getCapacity() < 16) {
      throw new RuntimeException("The buffers should be able to hold the complete header");
    }
    ByteBuffer byteBuffer = buffer.getByteBuffer();
    // now lets put the content of header in
    byteBuffer.putInt(sendMessage.getSource());
    // the path we are on, if not grouped it will be 0 and ignored
    byteBuffer.putInt(sendMessage.getFlags());
    byteBuffer.putInt(sendMessage.getDestintationIdentifier());
    // we add 0 for now and late change it
    byteBuffer.putInt(noOfMessage);
    // at this point we haven't put the length and we will do it at the serialization
    sendMessage.setWrittenHeaderSize(16);
    // lets set the size for 16 for now
    buffer.setSize(16);
  }

  /**
   * Serialized the message into the buffer
   * @param payload
   * @param sendMessage
   * @param buffer
   * @return true if the message is completely written
   */
  private boolean serializeMessage(MultiObject payload,
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

    int remaining = buffer.getCapacity();

    // we cannot use this buffer
    if (remaining < 6) {
      return;
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
        }
      } else {
        boolean complete = serializeMessage((MultiObject) o, sendMessage, buffer);
        if (complete) {
          state.setCurrentObject(i + 1);
        }
      }

      // check how much space left in this buffer
      remaining = buffer.getByteBuffer().remaining();
      if (!(remaining > 6 && state.getCurrentObject() < objectList.size())) {
        break;
      }
    }
    if (state.getCurrentObject() == objectList.size()) {
      sendMessage.setSendState(MPISendMessage.SendState.SERIALIZED);
    }
  }

  /**
   * Serialize a message in buffers.
   * @param message
   * @param state
   * @param targetBuffer
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
      needsCopy = currentMPIBuffer.getSize() - bytesCopiedFromSource;
      // 0th buffer has the header
      if (currentSourceBuffer == 0) {
        needsCopy -= 16;
        // we add 16 because,
        bytesCopiedFromSource += 16;
      }
      currentSourceByteBuffer.position(bytesCopiedFromSource);

      canCopy = needsCopy > targetRemainingSpace ? targetRemainingSpace : needsCopy;
      currentSourceByteBuffer.get(tempBytes, 0, canCopy);
      // todo check this method
      targetByteBuffer.put(tempBytes, 0, canCopy);
      totalBytes += canCopy;
      // the target buffer is full, we need to return
      if (needsCopy > targetRemainingSpace) {
        bytesCopiedFromSource += canCopy;
        break;
      } else {
        // we have more space in the target buffer, we can go to the next source buffer
        bytesCopiedFromSource += canCopy;
        targetRemainingSpace -= canCopy;
        currentSourceBuffer++;
      }
    }

    // set the data size of the target buffer
    targetBuffer.setSize(targetByteBuffer.position());
    state.setTotalBytes(totalBytes);
    if (currentSourceBuffer == buffers.size() && currentMPIBuffer != null
        && bytesCopiedFromSource == currentMPIBuffer.getSize()) {
      state.setBufferNo(0);
      state.setBytesCopied(0);
      return true;
    } else {
      state.setBufferNo(currentSourceBuffer);
      state.setBytesCopied(bytesCopiedFromSource);
      return false;
    }
  }

  private void buildMessageHeader(MPIBuffer buffer, int length, int source) {
    ByteBuffer byteBuffer = buffer.getByteBuffer();
    byteBuffer.putInt(length);
    byteBuffer.putShort((short) source);
  }

  /**
   * Serializes a java object using kryo serialization
   *
   * @param object
   * @param state
   * @param targetBuffer
   */
  public boolean serializeObject(MultiObject object, SerializeState state,
                              MPIBuffer targetBuffer) {
    byte[] data;
    int dataPosition = 0;
    ByteBuffer byteBuffer = targetBuffer.getByteBuffer();
    int totalBytes = state.getTotalBytes();
    if (state.getBytesCopied() == 0) {
      // okay we need to serialize the data
      data = serializer.serialize(object.getObject());
      // at this point we know the length of the data
      buildMessageHeader(targetBuffer, data.length, object.getSource());
      // add the header bytes to the total bytes
      totalBytes += 6;
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
}
