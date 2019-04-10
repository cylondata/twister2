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

import edu.iu.dsc.tws.comms.api.MessageType;

public final class ShortPacker implements PrimitivePacker<Short> {

  private static volatile ShortPacker instance;

  private ShortPacker() {

  }

  public static ShortPacker getInstance() {
    if (instance == null) {
      instance = new ShortPacker();
    }
    return instance;
  }

  @Override
  public MessageType<Short> getMessageType() {
    return MessageType.SHORT;
  }

  @Override
  public ByteBuffer addToBuffer(ByteBuffer byteBuffer, Short data) {
    return byteBuffer.putShort(data);
  }

  @Override
  public Short getFromBuffer(ByteBuffer byteBuffer, int offset) {
    return byteBuffer.getShort(offset);
  }

  @Override
  public Short getFromBuffer(ByteBuffer byteBuffer) {
    return byteBuffer.getShort();
  }
}
