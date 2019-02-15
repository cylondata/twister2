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
import java.util.logging.Logger;

import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.dfw.DataBuffer;
import edu.iu.dsc.tws.comms.dfw.OutMessage;
import edu.iu.dsc.tws.comms.dfw.io.types.DataSerializer;
import edu.iu.dsc.tws.comms.utils.KryoSerializer;

/**
 * Builds the message and copies it into data buffers.The structure of the message depends on the
 * type of message that is sent, for example if it is a keyed message or not.
 *
 * The main structure of the built message is |Header|Body|.
 *
 * The header has the following structure
 * |source|flags|destinationID|numberOfMessages|,
 * source - source of the message
 * flags - message flags
 * destinationId - where the message is sent
 * numberOfMessages - number of messages
 *
 * Header can be followed by 0 or more messages, each message will have the following structure
 * |length(integer)|message body|
 *
 * For a keyed message the message body consists of two parts
 * |key|body|
 *
 * For some keys we need to send the length of the key, i.e. byte arrays and objects. In that case
 * key consists of two parts
 * |key length(integer)|actual key|
 *
 * For other cases such as integer or double keys, we know the length of the key, so we only send
 * the key.
 */
public class UnifiedSerializer extends BaseSerializer {
  private static final Logger LOG = Logger.getLogger(UnifiedSerializer.class.getName());

  private MessageType dataType;

  public UnifiedSerializer(KryoSerializer serializer, int executor, MessageType dataType) {
    super(serializer, executor);
    this.serializer = serializer;
    LOG.fine("Initializing serializer on worker: " + executor);
  }


  /**
   * Builds the body of the message. Based on the message type different build methods are called
   *
   * @param payload the message that needs to be built
   * @param sendMessage the send message object that contains all the metadata
   * @param targetBuffer the data targetBuffer to which the built message needs to be copied
   * @return true if the body was built and copied to the targetBuffer successfully,false otherwise.
   */
  public boolean serializeSingleMessage(Object payload,
                                OutMessage sendMessage, DataBuffer targetBuffer) {
    MessageType type = sendMessage.getDataType();
    return serializeData(payload, sendMessage.getSerializationState(), targetBuffer, type);
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
