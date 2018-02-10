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
package edu.iu.dsc.tws.comms.mpi;

import java.nio.ByteBuffer;

import edu.iu.dsc.tws.comms.core.CommunicationContext;

import mpi.MPI;

public class MPIBuffer {
  private int capacity;
  private int size;
  private ByteBuffer byteBuffer;

  public MPIBuffer(int capacity) {
    this.capacity = capacity;
    this.byteBuffer = MPI.newByteBuffer(capacity);
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
