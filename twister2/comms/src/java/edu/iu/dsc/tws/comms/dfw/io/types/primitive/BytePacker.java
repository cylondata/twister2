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

public final class BytePacker implements PrimitivePacker<Byte> {

  private static volatile BytePacker instance;

  private BytePacker() {
  }

  public static BytePacker getInstance() {
    if (instance == null) {
      instance = new BytePacker();
    }
    return instance;
  }

  @Override
  public MessageType<Byte> getMessageType() {
    return MessageType.BYTE;
  }

  @Override
  public ByteBuffer addToBuffer(ByteBuffer byteBuffer, Byte data) {
    return byteBuffer.put(data);
  }

  @Override
  public ByteBuffer addToBuffer(ByteBuffer byteBuffer, int index, Byte data) {
    return byteBuffer.put(index, data);
  }


  @Override
  public Byte getFromBuffer(ByteBuffer byteBuffer, int offset) {
    return byteBuffer.get(offset);
  }

  @Override
  public Byte getFromBuffer(ByteBuffer byteBuffer) {
    return byteBuffer.get();
  }

  @Override
  public byte[] packToByteArray(Byte data) {
    return new byte[]{data};
  }
}
