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
package edu.iu.dsc.tws.comms.dfw;

import java.nio.ByteBuffer;

import edu.iu.dsc.tws.comms.core.CommunicationContext;

import mpi.MPI;

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

  public DataBuffer(int cap) {
    this.capacity = cap;
    this.byteBuffer = MPI.newByteBuffer(cap);
    this.byteBuffer.order(CommunicationContext.DEFAULT_BYTEORDER);
  }

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
}
