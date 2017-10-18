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
import java.util.Queue;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.MessageHeader;
import edu.iu.dsc.tws.comms.api.MessageSerializer;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.mpi.MPIBuffer;
import edu.iu.dsc.tws.comms.mpi.MPIMessage;
import edu.iu.dsc.tws.comms.mpi.MPISendMessage;

public class MPIMessageSerializer implements MessageSerializer {
  private Queue<MPIBuffer> sendBuffers;
  private KryoSerializer serializer;
  private Config config;
  private boolean grouped;

  public MPIMessageSerializer(Queue<MPIBuffer> buffers, KryoSerializer kryoSerializer) {
    this.sendBuffers = buffers;
    this.serializer = kryoSerializer;
  }

  @Override
  public void init(Config cfg, boolean grped) {
    this.config = cfg;
    this.grouped = grped;
  }

  @Override
  public Object build(Object message, Object partialBuildObject) {
    MPISendMessage sendMessage = (MPISendMessage) partialBuildObject;

    // we got an already serialized message, lets just return it
    if (sendMessage.getMPIMessage().isComplete()) {
      sendMessage.setSerializedState(MPISendMessage.SerializedState.FINISHED);
      return sendMessage;
    }

    while (sendBuffers.size() > 0 && sendMessage.serializedState()
        != MPISendMessage.SerializedState.FINISHED) {
      MPIBuffer buffer = sendBuffers.poll();

      if (sendMessage.serializedState() == MPISendMessage.SerializedState.INIT) {
        // build the header
        MessageHeader header = sendMessage.getMPIMessage().getHeader();
        buildHeader(header, buffer, sendMessage);
        sendMessage.setSerializedState(MPISendMessage.SerializedState.HEADER_BUILT);
      }

      if (sendMessage.serializedState() == MPISendMessage.SerializedState.HEADER_BUILT) {
        // build the body
        // first we need to serialize the body if needed
        serializeBody(message, sendMessage, buffer);
        sendMessage.setSerializedState(MPISendMessage.SerializedState.BODY);
      } else if (sendMessage.serializedState() == MPISendMessage.SerializedState.BODY) {
        // further build the body
        serializeBody(message, sendMessage, buffer);
      }

      // okay we are adding this buffer
      sendMessage.getMPIMessage().addBuffer(buffer);
      if (sendMessage.serializedState() == MPISendMessage.SerializedState.FINISHED) {
        MPIMessage mpiMessage = sendMessage.getMPIMessage();
        // mark the original message as complete
        mpiMessage.setComplete(true);
      }
    }
    return sendMessage;
  }

  private void buildHeader(MessageHeader header, MPIBuffer buffer,
                           MPISendMessage sendMessage) {
    if (buffer.getCapacity() < 24) {
      throw new RuntimeException("The buffers should be able to hold the complete header");
    }

    ByteBuffer byteBuffer = buffer.getByteBuffer();
    // now lets put the content of header in
    byteBuffer.putInt(header.getSourceId());
    byteBuffer.putInt(header.getDestId());
    byteBuffer.putInt(header.getEdge());
    byteBuffer.putInt(header.getLastNode());
    // todo : how to know the length
    byteBuffer.putInt(header.getLength());

    sendMessage.setWrittenHeaderSize(24);
    // todo: add properties to header
  }

  /**
   * Serialized the message into the buffer
   * @param payload
   * @param sendMessage
   * @param buffer
   * @return true if the message is completely written
   */
  private void serializeBody(Object payload,
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
        serializeObject(payload, sendMessage, buffer);
        break;
      case BYTE:
        break;
      case STRING:
        break;
      default:
        break;
    }
  }

  /**
   * Serializes a java object using kryo serialization
   *
   * @param object
   * @param sendMessage
   * @param buffer
   */
  private void serializeObject(Object object, MPISendMessage sendMessage, MPIBuffer buffer) {
    byte[] data;
    int dataPosition;
    if (sendMessage.serializedState() == MPISendMessage.SerializedState.HEADER_BUILT) {
      // okay we need to serialize the data
      data = serializer.serialize(object);
      dataPosition = 0;
    } else {
      data = sendMessage.getSendBytes();
      dataPosition = sendMessage.getByteCopied();
    }

    int remainingToCopy = data.length - dataPosition;
    // check how much space we have
    ByteBuffer byteBuffer = buffer.getByteBuffer();
    int bufferSpace = byteBuffer.capacity() - byteBuffer.position();

    int copyBytes = remainingToCopy > bufferSpace ? bufferSpace : remainingToCopy;
    // check how much space left in the buffer
    byteBuffer.put(data, dataPosition, copyBytes);
    sendMessage.setByteCopied(dataPosition + copyBytes);
    // okay we are done with the message
    if (copyBytes == remainingToCopy) {
      sendMessage.setSerializedState(MPISendMessage.SerializedState.FINISHED);
    }
  }
}
