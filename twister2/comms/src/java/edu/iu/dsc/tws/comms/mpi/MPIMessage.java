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

  /**
   * Keeps the number of references to this message
   * The resources associated with the message is released when refcount becomes 0
   */
  private int refCount;

  /**
   * Type of the message, weather request or send
   */
  private MPIMessageType messageType;

  private MPIMessageReleaseCallback releaseListener;

  private final int edge;

  /**
   * Keep track of the originating id, this is required to release the buffers allocated.
   */
  private final int originatingId;

  public MPIMessage(int originatingId, int stream,
                    MPIMessageType type, MPIMessageReleaseCallback releaseListener) {
    this(originatingId, stream, 1, type, releaseListener);
  }

  public MPIMessage(int originatingId, int stream, int refCount,
                    MPIMessageType type, MPIMessageReleaseCallback releaseListener) {
    this.edge = stream;
    this.refCount = refCount;
    this.messageType = type;
    this.releaseListener = releaseListener;
    this.originatingId = originatingId;
  }

  public int getEdge() {
    return edge;
  }

  public List<MPIBuffer> getBuffers() {
    return buffers;
  }

  public int incrementRefCount() {
    refCount++;
    return refCount;
  }

  public int incrementRefCount(int count) {
    refCount += count;
    return refCount;
  }

  public MPIMessageType getMessageType() {
    return messageType;
  }

  public boolean doneProcessing() {
    return refCount == 0;
  }
  /**
   * Release the allocated resources to this buffer.
   */
  public void release() {
    refCount--;
    if (refCount == 0) {
      releaseListener.release(this);
    }
  }

  public int getOriginatingId() {
    return originatingId;
  }
}
