//
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

import java.util.ArrayList;
import java.util.List;

public class MPIMessage {
  private final List<MPIBuffer> buffers = new ArrayList<MPIBuffer>();

  private final TWSMPIChannel channel;
  /**
   * Keeps the number of references to this message
   * The resources associated with the message is released when refcount becomes 0
   */
  private int refCount;

  public MPIMessage(TWSMPIChannel channel) {
    this(channel, 1);
  }

  public MPIMessage(TWSMPIChannel channel, int refCount) {
    this.channel = channel;
    this.refCount = refCount;
  }

  public List<MPIBuffer> getBuffers() {
    return buffers;
  }

  public int getRefCount() {
    return refCount;
  }

  public int incrementRefCount() {
    refCount++;
    return refCount;
  }

  /**
   * Release the allocated resources to this buffer.
   */
  public void release() {
    refCount--;
    if (refCount == 0) {
      channel.releaseMessage(this);
    }
  }
}
