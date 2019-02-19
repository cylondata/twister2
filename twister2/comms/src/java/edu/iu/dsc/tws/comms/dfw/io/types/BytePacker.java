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
package edu.iu.dsc.tws.comms.dfw.io.types;

import java.nio.ByteBuffer;

import edu.iu.dsc.tws.comms.api.DataPacker;
import edu.iu.dsc.tws.comms.dfw.DataBuffer;
import edu.iu.dsc.tws.comms.dfw.InMessage;
import edu.iu.dsc.tws.comms.dfw.io.SerializeState;

public class BytePacker implements DataPacker {
  public BytePacker() {
  }

  @Override
  public int packData(Object data, SerializeState state) {
    if (state.getData() == null) {
      state.setData((byte[]) data);
    }
    return state.getData().length;
  }

  @Override
  public boolean writeDataToBuffer(Object data,
                                   ByteBuffer targetBuffer, SerializeState state) {
    return DataSerializer.copyDataBytes(targetBuffer, state);
  }

  public Object initializeUnPackDataObject(int length) {
    return new byte[length];
  }

  @Override
  public int readDataFromBuffer(InMessage currentMessage, int currentLocation,
                                DataBuffer buffer, int currentObjectLength) {
    int startIndex = currentMessage.getUnPkCurrentBytes();
    startIndex = startIndex / Integer.BYTES;
    byte[] val = (byte[]) currentMessage.getDeserializingObject();
    return PartialDataDeserializer.deserializeByte(buffer, currentObjectLength,
        val, startIndex, currentLocation);
  }
}
