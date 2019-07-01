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

public final class IntegerPacker implements PrimitivePacker<Integer> {

  private static volatile IntegerPacker instance;

  private IntegerPacker() {
  }

  public static IntegerPacker getInstance() {
    if (instance == null) {
      instance = new IntegerPacker();
    }
    return instance;
  }

  @Override
  public MessageType<Integer, Integer> getMessageType() {
    return MessageTypes.INTEGER;
  }

  @Override
  public ByteBuffer addToBuffer(ByteBuffer byteBuffer, Integer data) {
    return byteBuffer.putInt(data);
  }

  @Override
  public ByteBuffer addToBuffer(ByteBuffer byteBuffer, int index, Integer data) {
    return byteBuffer.putInt(index, data);
  }

  @Override
  public Integer getFromBuffer(ByteBuffer byteBuffer, int offset) {
    return byteBuffer.getInt(offset);
  }

  @Override
  public Integer getFromBuffer(ByteBuffer byteBuffer) {
    return byteBuffer.getInt();
  }
}
