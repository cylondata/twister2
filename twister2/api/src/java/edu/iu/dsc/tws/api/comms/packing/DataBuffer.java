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
package edu.iu.dsc.tws.api.comms.packing;

import java.nio.ByteBuffer;

import edu.iu.dsc.tws.api.comms.CommunicationContext;

/**
 * Keep track of a byte buffer with capacity and size
 */
public class DataBuffer {
  /**
   * The capacity of the buffer
   */
  private final int capacity;
  /**
   * Size of the buffer
   */
  private int size = 0;
  /**
   * The actual buffer
   */
  private ByteBuffer byteBuffer;

  /**
   * Create a buffer
   *
   * @param buffer the underlying byte buffer
   */
  public DataBuffer(ByteBuffer buffer) {
    this.capacity = buffer.capacity();
    this.byteBuffer = buffer;
    this.byteBuffer.order(CommunicationContext.DEFAULT_BYTEORDER);
  }

  public void setSize(int size) {
    this.size = size;
  }

  public int getCapacity() {
    return capacity;
  }

  public int getSize() {
    return size;
  }

  public ByteBuffer getByteBuffer() {
    return byteBuffer;
  }

  /**
   * Copies a part of the ByteBuffer to the given byte array
   *
   * @param bufferLocation starting position of byteBuffer
   * @param value byte array to copy data
   * @param startIndex initial index of the byte array
   * @param byteLength no of bytes top copy
   * @return amount of bytes read
   */
  public int copyPartToByteArray(int bufferLocation,
                                 byte[] value,
                                 int startIndex,
                                 int byteLength) {
    int bytesRead = 0;
    int currentBufferLocation = bufferLocation;
    for (int i = startIndex; i < byteLength; i++) {
      int remaining = size - currentBufferLocation;
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
