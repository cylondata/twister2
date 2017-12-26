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
  private Config config;
  private ObjectSerializer objectSerializer;

  public MPIMultiMessageSerializer(Queue<MPIBuffer> buffers, KryoSerializer kryoSerializer) {
    this.sendBuffers = buffers;
    this.serializer = kryoSerializer;
    this.objectSerializer = new ObjectSerializer(serializer);
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
    if (sendMessage.getMPIMessage().isComplete()) {
      sendMessage.setSendState(MPISendMessage.SendState.SERIALIZED);
      return sendMessage;
    }

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
      sendMessage.getMPIMessage().addBuffer(buffer);
      if (sendMessage.serializedState() == MPISendMessage.SendState.SERIALIZED) {
        MPIMessage mpiMessage = sendMessage.getMPIMessage();
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
  private void serializeMessage(Object payload,
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
        objectSerializer.serializeObject(payload, sendMessage, buffer);
        break;
      case BYTE:
        break;
      case STRING:
        break;
      case BUFFER:
        break;
      default:
        break;
    }
  }

  @SuppressWarnings("rawtypes")
  private void serializeBody(Object object, MPISendMessage sendMessage, MPIBuffer buffer) {
    List objectList = (List) object;
    SerializeState state =
        (SerializeState) sendMessage.getSerializationState();

    int startIndex = state.getCurrentObject();

    int remaining = buffer.getCapacity();
    int copyBytes = 0;

    ByteBuffer byteBuffer = buffer.getByteBuffer();
    for (int i = startIndex; i < objectList.size(); i++) {
      Object o = objectList.get(i);
      if (i == 0) {
        // we have the header at the begining
        remaining -= 16;
        // we put the number of messages here
        byteBuffer.putInt(12, objectList.size());
      }

      if (o instanceof MPIMessage) {
        MPIMessage mpiMessage = (MPIMessage) o;
        int bufferIndex = state.getBufferNo();
        // we need to copy the buffers
        List<MPIBuffer> buffers = mpiMessage.getBuffers();
        for (int j = bufferIndex; j < buffers.size(); j++) {
          MPIBuffer mpiBuffer = buffers.get(j);
        }
      } else {
        serializeMessage(o, sendMessage, buffer);
      }
    }
  }

  private void buildMessageHeader(MPIBuffer buffer, int offset, int length, int source) {
    ByteBuffer byteBuffer = buffer.getByteBuffer();
    byteBuffer.putInt(offset, length);
    byteBuffer.putInt(offset + 4, source);
  }
}
