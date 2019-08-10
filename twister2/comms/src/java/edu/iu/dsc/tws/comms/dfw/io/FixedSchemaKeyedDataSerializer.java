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

import edu.iu.dsc.tws.api.comms.packing.DataBuffer;
import edu.iu.dsc.tws.api.comms.packing.DataPacker;
import edu.iu.dsc.tws.api.comms.packing.MessageSchema;

/**
 * This serializer will be used to serialize messages with keys
 */
public class FixedSchemaKeyedDataSerializer extends KeyedDataSerializer {

  private MessageSchema messageSchema;

  public FixedSchemaKeyedDataSerializer(MessageSchema messageSchema) {
    this.messageSchema = messageSchema;
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
  protected boolean serializeKeyedData(Object payload, Object key,
                                       DataPacker dataPacker, DataPacker keyPacker,
                                       SerializeState state,
                                       DataBuffer targetBuffer) {
    ByteBuffer byteBuffer = targetBuffer.getByteBuffer();
    // okay we need to serialize the header
    if (state.getPart() == SerializeState.Part.INIT) {
      int keyLength = messageSchema.getKeySize();
      state.getActive().setTotalToCopy(keyLength);

      // now swap the data store.
      state.swap();

      // okay we need to serialize the data
      int dataLength = messageSchema.getMessageSize() - messageSchema.getKeySize();
      state.setCurrentHeaderLength(dataLength + keyLength);
      state.getActive().setTotalToCopy(dataLength);

      state.swap(); //next we will be processing key, so need to swap

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
}
