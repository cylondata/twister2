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
package edu.iu.dsc.tws.api.comms.packing.types.primitive;

import java.nio.ByteBuffer;

import edu.iu.dsc.tws.api.comms.messaging.types.MessageType;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;

public final class IntegerArrayPacker implements PrimitiveArrayPacker<int[]> {

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
  public MessageType<int[], int[]> getMessageType() {
    return MessageTypes.INTEGER_ARRAY;
  }

  @Override
  public ByteBuffer addToBuffer(ByteBuffer byteBuffer, int[] data, int index) {
    return byteBuffer.putInt(data[index]);
  }

  @Override
  public ByteBuffer addToBuffer(ByteBuffer byteBuffer, int offset, int[] data, int index) {
    return byteBuffer.putInt(offset, data[index]);
  }

  @Override
  public void readFromBufferAndSet(ByteBuffer byteBuffer, int offset, int[] array, int index) {
    array[index] = byteBuffer.getInt(offset);
  }

  @Override
  public void readFromBufferAndSet(ByteBuffer byteBuffer, int[] array, int index) {
    array[index] = byteBuffer.getInt();
  }

  @Override
  public int[] wrapperForLength(int length) {
    return new int[length];
  }
}
