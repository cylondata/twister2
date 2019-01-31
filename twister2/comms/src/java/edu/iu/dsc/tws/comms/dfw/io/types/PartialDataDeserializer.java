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
import java.util.List;

import edu.iu.dsc.tws.comms.dfw.DataBuffer;

public final class PartialDataDeserializer {
  private PartialDataDeserializer() {
  }

  private static void deserializeInteger(List<DataBuffer> buffers, int noOfInts,
                                         int[] value, int startIndex) {
    int bufferIndex = 0;
    for (int i = startIndex; i < noOfInts; i++) {
      ByteBuffer byteBuffer = buffers.get(bufferIndex).getByteBuffer();
      int remaining = byteBuffer.remaining();
      if (remaining >= Integer.BYTES) {
        value[i] = byteBuffer.getInt();
      } else {
        bufferIndex = getReadBuffer(buffers, Integer.BYTES, bufferIndex);
        if (bufferIndex < 0) {
          throw new RuntimeException("We should always have the ints");
        }
      }
    }
  }

  private static int getReadBuffer(List<DataBuffer> bufs, int size, int currentBufferIndex) {
    for (int i = currentBufferIndex; i < bufs.size(); i++) {
      ByteBuffer byteBuffer = bufs.get(i).getByteBuffer();
      // now check if we need to go to the next buffer
      if (byteBuffer.remaining() > size) {
        // if we are at the end we need to move to next
        byteBuffer.rewind();
        return i;
      }
    }
    return -1;
  }
}
