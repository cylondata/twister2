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

import edu.iu.dsc.tws.comms.api.MessageType;

public final class LongArrayPacker implements PrimitiveArrayPacker<long[]> {

  private static volatile LongArrayPacker instance;

  private LongArrayPacker() {

  }

  public static LongArrayPacker getInstance() {
    if (instance == null) {
      instance = new LongArrayPacker();
    }
    return instance;
  }

  @Override
  public MessageType<long[]> getMessageType() {
    return MessageType.LONG_ARRAY;
  }

  @Override
  public ByteBuffer addToBuffer(ByteBuffer byteBuffer, long[] data, int index) {
    return byteBuffer.putLong(data[index]);
  }

  @Override
  public void readFromBufferAndSet(ByteBuffer byteBuffer, int offset, long[] array, int index) {
    array[index] = byteBuffer.getLong(offset);
  }

  @Override
  public void readFromBufferAndSet(ByteBuffer byteBuffer, long[] array, int index) {
    array[index] = byteBuffer.getLong();
  }

  @Override
  public long[] wrapperForLength(int length) {
    return new long[length];
  }
}
