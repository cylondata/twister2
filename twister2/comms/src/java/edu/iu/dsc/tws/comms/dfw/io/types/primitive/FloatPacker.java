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

public final class FloatPacker implements PrimitivePacker<Float> {

  private static volatile FloatPacker instance;

  private FloatPacker() {
  }

  public static FloatPacker getInstance() {
    if (instance == null) {
      instance = new FloatPacker();
    }
    return instance;
  }

  @Override
  public MessageType<Float> getMessageType() {
    return MessageType.FLOAT;
  }

  @Override
  public ByteBuffer addToBuffer(ByteBuffer byteBuffer, Float data) {
    return byteBuffer.putFloat(data);
  }

  @Override
  public Float getFromBuffer(ByteBuffer byteBuffer, int offset) {
    return byteBuffer.getFloat(offset);
  }

  @Override
  public Float getFromBuffer(ByteBuffer byteBuffer) {
    return byteBuffer.getFloat();
  }
}
