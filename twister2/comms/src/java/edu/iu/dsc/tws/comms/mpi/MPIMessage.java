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

import edu.iu.dsc.tws.comms.api.MessageHeader;
import edu.iu.dsc.tws.comms.api.MessageType;

public class MPIMessage {
  /**
   * List of buffers filled with the message
   */
  private final List<MPIBuffer> buffers = new ArrayList<MPIBuffer>();

  /**
   * Keeps the number of references to this message
   * The resources associated with the message is released when refcount becomes 0
   */
  private int refCount;

  /**
   * Type of the message, weather request or send
   */
  private MPIMessageDirection messageDirection;


  private MPIMessageReleaseCallback releaseListener;

  /**
   * Keep track of the originating id, this is required to release the buffers allocated.
   */
  private int originatingId;

  /**
   * The message header
   */
  private MessageHeader header;

  /**
   * Keep weather the message has been fully built
   */
  private boolean complete = false;

  /**
   * Message type
   */
  private MessageType type;

  /**
   * Number of bytes in the header
   */
  private int headerSize;

  public MPIMessage() {
  }

  public MPIMessage(int originatingId, MessageType messageType,
                    MPIMessageDirection messageDirection,
                    MPIMessageReleaseCallback releaseListener) {
    this.refCount = 0;
    this.messageDirection = messageDirection;
    this.releaseListener = releaseListener;
    this.originatingId = originatingId;
    this.complete = true;
    this.type = messageType;
  }

  public MPIMessage(int originatingId, MessageType messageType, MessageHeader header, int refCount,
                    MPIMessageDirection type, MPIMessageReleaseCallback releaseListener) {
    this.type = messageType;
    this.header = header;
    this.refCount = refCount;
    this.messageDirection = type;
    this.releaseListener = releaseListener;
    this.originatingId = originatingId;
    this.complete = true;
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

  public MPIMessageDirection getMessageDirection() {
    return messageDirection;
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

  public void addBuffer(MPIBuffer buffer) {
    buffers.add(buffer);
  }

  public int getOriginatingId() {
    return originatingId;
  }

  public MessageHeader getHeader() {
    return header;
  }

  public void setHeader(MessageHeader header) {
    this.header = header;
  }

  public boolean build() {
    if (header == null && buffers.size() > 0) {
      return false;
    }

    if (header != null) {
      int currentSize = 0;
      for (MPIBuffer buffer : buffers) {
        currentSize += buffer.getByteBuffer().remaining();
      }
      if (currentSize == header.getLength()) {
        complete = true;
        return true;
      }
    }
    return false;
  }

  public boolean isComplete() {
    return complete;
  }

  public void setComplete(boolean complete) {
    this.complete = complete;
  }

  public MessageType getType() {
    return type;
  }

  public void setHeaderSize(int headerSize) {
    this.headerSize = headerSize;
  }

  public int getHeaderSize() {
    return headerSize;
  }
}
