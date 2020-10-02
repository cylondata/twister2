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

public final class ShortArrayPacker implements PrimitiveArrayPacker<short[]> {

  private static volatile ShortArrayPacker instance;

  private ShortArrayPacker() {

  }

  public static ShortArrayPacker getInstance() {
    if (instance == null) {
      instance = new ShortArrayPacker();
    }
    return instance;
  }

  @Override
  public boolean bulkReadFromBuffer(ByteBuffer buffer, short[] dest, int offset, int length) {
    buffer.asShortBuffer().get(dest, offset, length);
    return true;
  }

  @Override
  public boolean bulkCopyToBuffer(short[] src, ByteBuffer buffer, int offset, int length) {
    buffer.asShortBuffer().put(src, offset, length);
    return true;
  }

  @Override
  public MessageType<short[], short[]> getMessageType() {
    return MessageTypes.SHORT_ARRAY;
  }

  @Override
  public ByteBuffer addToBuffer(ByteBuffer byteBuffer, short[] data, int index) {
    return byteBuffer.putShort(data[index]);
  }

  @Override
  public ByteBuffer addToBuffer(ByteBuffer byteBuffer, int offset, short[] data, int index) {
    return byteBuffer.putShort(offset, data[index]);
  }

  @Override
  public void readFromBufferAndSet(ByteBuffer byteBuffer, int offset, short[] array, int index) {
    array[index] = byteBuffer.getShort(offset);
  }

  @Override
  public void readFromBufferAndSet(ByteBuffer byteBuffer, short[] array, int index) {
    array[index] = byteBuffer.getShort();
  }

  @Override
  public short[] wrapperForLength(int length) {
    return new short[length];
  }
}
