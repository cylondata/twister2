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

public class ShortPacker implements DataPacker {
  public ShortPacker() {
  }

  @Override
  public int packData(Object data, SerializeState state) {
    return ((short[]) data).length * Short.BYTES;
  }

  @Override
  public boolean writeDataToBuffer(Object data,
                                   ByteBuffer targetBuffer, SerializeState state) {
    return DataSerializer.copyShort((short[]) data, targetBuffer, state);
  }

  public Object initializeUnPackDataObject(int length) {
    return new short[length / Short.BYTES];
  }

  @Override
  public int readDataFromBuffer(InMessage currentMessage, int currentLocation,
                                DataBuffer buffer, int currentObjectLength) {
    int startIndex = currentMessage.getUnPkCurrentBytes();
    startIndex = startIndex / Short.BYTES;
    short[] val = (short[]) currentMessage.getDeserializingObject();
    return PartialDataDeserializer.deserializeShort(buffer, currentObjectLength,
        val, startIndex, currentLocation);
  }
}
