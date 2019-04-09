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
package edu.iu.dsc.tws.comms.dfw.io.types;

import java.nio.ByteBuffer;

import edu.iu.dsc.tws.comms.dfw.DataBuffer;

public final class PartialDataDeserializer {
  private PartialDataDeserializer() {
  }

  public static int deserializeByte(DataBuffer buffers, int byteLength,
                                     byte[] value, int startIndex, int bufferLocation) {
    int bytesRead = 0;
    int currentBufferLocation = bufferLocation;
    for (int i = startIndex; i < byteLength; i++) {
      ByteBuffer byteBuffer = buffers.getByteBuffer();
      int remaining = buffers.getSize() - currentBufferLocation;
      if (remaining >= 1) {
        value[i] = byteBuffer.get(currentBufferLocation);
        bytesRead += 1;
        currentBufferLocation += 1;
      } else {
        break;
      }
    }
    return bytesRead;
  }
}
