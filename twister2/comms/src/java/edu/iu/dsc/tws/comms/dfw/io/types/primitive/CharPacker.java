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

public final class CharPacker implements PrimitivePacker<Character> {

  private static volatile CharPacker instance;

  private CharPacker() {
  }

  public static CharPacker getInstance() {
    if (instance == null) {
      instance = new CharPacker();
    }
    return instance;
  }

  @Override
  public MessageType<Character> getMessageType() {
    return MessageType.CHAR;
  }

  @Override
  public ByteBuffer addToBuffer(ByteBuffer byteBuffer, Character data) {
    return byteBuffer.putChar(data);
  }

  @Override
  public ByteBuffer addToBuffer(ByteBuffer byteBuffer, int index, Character data) {
    return byteBuffer.putChar(index, data);
  }

  @Override
  public Character getFromBuffer(ByteBuffer byteBuffer, int offset) {
    return byteBuffer.getChar(offset);
  }

  @Override
  public Character getFromBuffer(ByteBuffer byteBuffer) {
    return byteBuffer.getChar();
  }
}
