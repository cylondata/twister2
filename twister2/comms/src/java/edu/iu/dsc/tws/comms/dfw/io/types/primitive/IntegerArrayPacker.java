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
package edu.iu.dsc.tws.comms.dfw.io.types.primitive;

import java.nio.ByteBuffer;

import edu.iu.dsc.tws.comms.api.ArrayPacker;
import edu.iu.dsc.tws.comms.dfw.DataBuffer;
import edu.iu.dsc.tws.comms.dfw.InMessage;
import edu.iu.dsc.tws.comms.dfw.io.SerializeState;
import edu.iu.dsc.tws.comms.dfw.io.types.DataSerializer;
import edu.iu.dsc.tws.comms.dfw.io.types.PartialDataDeserializer;

public final class IntegerArrayPacker implements ArrayPacker<int[]> {

  private static volatile IntegerArrayPacker instance;

  private IntegerArrayPacker() {
  }

  public static IntegerArrayPacker getInstance() {
    if (instance == null) {
      instance = new IntegerArrayPacker();
    }
    return instance;
  }

  @Override
  public int packData(int[] data, SerializeState state) {
    return data.length * Integer.BYTES;
  }

  @Override
  public boolean writeDataToBuffer(int[] data,
                                   ByteBuffer targetBuffer, SerializeState state) {
    return DataSerializer.copyIntegers(data, targetBuffer, state);
  }


  public int[] initializeUnPackDataObject(int length) {
    return this.wrapperForByteLength(length);
  }

  @Override
  public int readDataFromBuffer(InMessage currentMessage, int currentLocation,
                                DataBuffer buffer, int currentObjectLength) {
    int startIndex = currentMessage.getUnPkCurrentBytes();
    startIndex = startIndex / Integer.BYTES;
    int[] val = (int[]) currentMessage.getDeserializingObject();
    return PartialDataDeserializer.deserializeInteger(buffer, currentObjectLength,
        val, startIndex, currentLocation);
  }

  @Override
  public int[] wrapperForByteLength(int byteLength) {
    return new int[byteLength / Integer.BYTES];
  }
}
