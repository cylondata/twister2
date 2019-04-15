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
import edu.iu.dsc.tws.common.kryo.KryoSerializer;
import edu.iu.dsc.tws.comms.api.DataPacker;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.dfw.DataBuffer;
import edu.iu.dsc.tws.comms.dfw.OutMessage;

/**
 * This serializer will be used to serialize messages with keys
 */
public class KeyedSerializer extends BaseSerializer {
  private static final Logger LOG = Logger.getLogger(KeyedSerializer.class.getName());

  private DataPacker dataPacker;

  private DataPacker keyPacker;

  public KeyedSerializer(KryoSerializer serializer, int executor,
                         MessageType keyType, MessageType dataType) {
    super(serializer, executor);
    this.serializer = serializer;
    dataPacker = dataType.getDataPacker();
    keyPacker = keyType.getDataPacker();
    LOG.fine("Initializing serializer on worker: " + executor);
  }

  @Override
  public void init(Config cfg, Queue<DataBuffer> buffers, boolean k) {
    this.sendBuffers = buffers;
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
    Tuple tuple = (Tuple) payload;
    return serializeKeyedData(tuple.getValue(), tuple.getKey(),
        sendMessage.getSerializationState(), targetBuffer);
  }

  /**
   * Helper method that builds the body of the message for keyed messages.
   *
   * @param payload the message that needs to be built
   * @param key the key associated with the message
   * @param state the state object of the message
   * @param targetBuffer the data targetBuffer to which the built message needs to be copied
   * @return true if the body was built and copied to the targetBuffer successfully,false otherwise.
   */
  private boolean serializeKeyedData(Object payload, Object key, SerializeState state,
                                     DataBuffer targetBuffer) {
    ByteBuffer byteBuffer = targetBuffer.getByteBuffer();
    // okay we need to serialize the header
    if (state.getPart() == SerializeState.Part.INIT) {
      int keyLength = keyPacker.determineLength(key, state);
      state.getActive().setTotalToCopy(keyLength);

      // now swap the data store.
      state.swap();

      // okay we need to serialize the data
      int dataLength = dataPacker.determineLength(payload, state);
      state.setCurrentHeaderLength(dataLength + keyLength);
      state.getActive().setTotalToCopy(dataLength);

      state.swap(); //next we will be processing key, so need to swap

      state.setPart(SerializeState.Part.HEADER);
    }

    if (state.getPart() == SerializeState.Part.HEADER) {
      // first we need to copy the data size to buffer
      if (buildSubMessageHeader(targetBuffer, state.getCurrentHeaderLength())) {
        // now set the size of the buffer
        targetBuffer.setSize(byteBuffer.position());
        return false;
      }
      state.setPart(SerializeState.Part.KEY);
    }

    if (state.getPart() == SerializeState.Part.KEY) {
      // this call will copy the key length to buffer as well
      boolean complete = DataPackerProxy.writeDataToBuffer(
          keyPacker,
          key,
          byteBuffer,
          state
      );
      // now set the size of the buffer
      targetBuffer.setSize(byteBuffer.position());
      if (complete) {
        state.swap(); //if key is done, swap to activate saved data object
        state.setPart(SerializeState.Part.BODY);
      }
    }

    // now we can serialize the body
    if (state.getPart() != SerializeState.Part.BODY) {
      // now set the size of the buffer
      targetBuffer.setSize(byteBuffer.position());
      return false;
    }

    // now lets copy the actual data
    boolean completed = DataPackerProxy.writeDataToBuffer(
        dataPacker,
        payload,
        byteBuffer,
        state
    );
    // now set the size of the buffer
    targetBuffer.setSize(byteBuffer.position());

    // okay we are done with the message
    return state.reset(completed);
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
