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
import edu.iu.dsc.tws.comms.dfw.OutMessage;

public class FixedSchemaDataSerializer extends DataSerializer {

  private MessageSchema messageSchema;

  public FixedSchemaDataSerializer(MessageSchema messageSchema) {
    this.messageSchema = messageSchema;
  }

  /**
   * Helper method that builds the body of the message for regular messages.
   *
   * @param payload the message that needs to be built
   * @param state the state object of the message
   * @param targetBuffer the data targetBuffer to which the built message needs to be copied
   * @return true if the body was built and copied to the targetBuffer successfully,false otherwise.
   */
  protected boolean serializeData(Object payload, SerializeState state,
                                  DataBuffer targetBuffer, DataPacker dataPacker) {
    ByteBuffer byteBuffer = targetBuffer.getByteBuffer();
    // okay we need to serialize the header
    if (state.getPart() == SerializeState.Part.INIT) {
      // okay we need to serialize the data
      int dataLength = this.messageSchema.getMessageSize();
      state.getActive().setTotalToCopy(dataLength);

      // no header is required due to fixed schema
      state.setPart(SerializeState.Part.BODY);
    }

    // now we can serialize the body
    if (state.getPart() != SerializeState.Part.BODY) {
      // now set the size of the buffer
      targetBuffer.setSize(byteBuffer.position());
      return false;
    }

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
