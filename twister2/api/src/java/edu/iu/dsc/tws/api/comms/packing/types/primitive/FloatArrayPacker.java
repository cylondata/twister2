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
package edu.iu.dsc.tws.api.comms.packing.types.primitive;

import java.nio.ByteBuffer;

import edu.iu.dsc.tws.api.comms.messaging.types.MessageType;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;

public final class FloatArrayPacker implements PrimitiveArrayPacker<float[]> {

  private static volatile FloatArrayPacker instance;

  private FloatArrayPacker() {
  }

  public static FloatArrayPacker getInstance() {
    if (instance == null) {
      instance = new FloatArrayPacker();
    }
    return instance;
  }

  @Override
  public MessageType<float[], float[]> getMessageType() {
    return MessageTypes.FLOAT_ARRAY;
  }

  @Override
  public ByteBuffer addToBuffer(ByteBuffer byteBuffer, float[] data, int index) {
    return byteBuffer.putFloat(data[index]);
  }

  @Override
  public ByteBuffer addToBuffer(ByteBuffer byteBuffer, int offset, float[] data, int index) {
    return byteBuffer.putFloat(offset, data[index]);
  }

  @Override
  public void readFromBufferAndSet(ByteBuffer byteBuffer, int offset, float[] array, int index) {
    array[index] = byteBuffer.getFloat(offset);
  }

  @Override
  public void readFromBufferAndSet(ByteBuffer byteBuffer, float[] array, int index) {
    array[index] = byteBuffer.getFloat();
  }

  @Override
  public float[] wrapperForLength(int length) {
    return new float[length];
  }
}
