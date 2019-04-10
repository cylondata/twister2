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

public final class DoublePacker implements PrimitivePacker<Double> {

  private static volatile DoublePacker instance;

  private DoublePacker() {
  }

  public static DoublePacker getInstance() {
    if (instance == null) {
      instance = new DoublePacker();
    }
    return instance;
  }

  @Override
  public MessageType<Double> getMessageType() {
    return MessageType.DOUBLE;
  }

  @Override
  public ByteBuffer addToBuffer(ByteBuffer byteBuffer, Double data) {
    return byteBuffer.putDouble(data);
  }

  @Override
  public ByteBuffer addToBuffer(ByteBuffer byteBuffer, int index, Double data) {
    return byteBuffer.putDouble(index, data);
  }

  @Override
  public Double getFromBuffer(ByteBuffer byteBuffer, int offset) {
    return byteBuffer.getDouble(offset);
  }

  @Override
  public Double getFromBuffer(ByteBuffer byteBuffer) {
    return byteBuffer.getDouble();
  }
}
