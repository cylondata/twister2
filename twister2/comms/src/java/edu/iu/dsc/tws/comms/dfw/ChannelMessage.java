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

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import edu.iu.dsc.tws.comms.api.MessageHeader;
import edu.iu.dsc.tws.comms.api.MessageType;

public class ChannelMessage {
  private static final Logger LOG = Logger.getLogger(ChannelMessage.class.getName());
  /**
   * List of buffers filled with the message
   */
  private final List<DataBuffer> buffers = new ArrayList<DataBuffer>();

  /**
   * List of byte arrays which are used to copy data from {@link ChannelMessage#buffers}
   * When the system runs out of receive buffers
   */
  private final List<DataBuffer> overflowBuffers = new ArrayList<DataBuffer>();

  /**
   * Keeps the number of references to this message
   * The resources associated with the message is released when refcount becomes 0
   */
  private int refCount;

  /**
   * Type of the message, weather request or send
   */
  private MessageDirection messageDirection;


  private ChannelMessageReleaseCallback releaseListener;

  /**
   * Keep track of the originating id, this is required to release the buffers allocated.
   */
  private int originatingId;

  /**
   * The message header
   */
  private MessageHeader header;

  /**
   * Keeps track of whether header of the object contained in the buffer was sent or not.
   * This is only used when a message is broken down into several buffers and each buffer is sent
   * separately
   */
  private boolean headerSent;

  /**
   * Keep whether the current message is a partial object. This depends on whether all the data
   * is copied into the buffers or not. This is used when large messages are broken down into
   * several smaller messages
   */
  private boolean isPartial;

  /**
   * Keep whether the message has been fully built
   */
  private boolean complete = false;

  /**
   * Message type
   */
  private MessageType type;

  /**
   * If a keyed message, the key being used
   */
  private MessageType keyType = MessageType.INTEGER;

  /**
   * Number of bytes in the header
   */
  private int headerSize;

  public enum ReceivedState {
    INIT,
    DOWN,
    RECEIVE
  }

  /**
   * Received state
   */
  private ReceivedState receivedState;

  public ChannelMessage() {
  }

  public ChannelMessage(int originatingId, MessageType messageType,
                        MessageDirection messageDirection,
                        ChannelMessageReleaseCallback releaseListener) {
    this.refCount = 0;
    this.messageDirection = messageDirection;
    this.releaseListener = releaseListener;
    this.originatingId = originatingId;
    this.complete = false;
    this.type = messageType;
    this.receivedState = ReceivedState.INIT;
  }

  public List<DataBuffer> getBuffers() {
    if (overflowBuffers.size() > 0) {
      List<DataBuffer> total = new ArrayList<DataBuffer>();
      total.addAll(overflowBuffers);
      total.addAll(buffers);
      return total;
    } else {
      return buffers;
    }
  }

  /**
   * returns the direct buffers that were allocated.
   */
  public List<DataBuffer> getNormalBuffers() {
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

  public MessageDirection getMessageDirection() {
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

  public void addBuffer(DataBuffer buffer) {
    buffers.add(buffer);
  }

  protected void addBuffers(List<DataBuffer> bufferList) {
    buffers.addAll(bufferList);
  }

  protected void addOverFlowBuffers(List<DataBuffer> bufferList) {
    overflowBuffers.addAll(bufferList);
  }

  protected void removeAllBuffers() {
    overflowBuffers.clear();
    buffers.clear();
  }

  public void addToOverFlowBuffer(DataBuffer data) {
    overflowBuffers.add(data);
  }

  public List<DataBuffer> getOverflowBuffers() {
    return overflowBuffers;
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

  public void setOriginatingId(int originatingId) {
    this.originatingId = originatingId;
  }

  public void setMessageDirection(MessageDirection messageDirection) {
    this.messageDirection = messageDirection;
  }

  public ChannelMessageReleaseCallback getReleaseListener() {
    return releaseListener;
  }

  public void setReleaseListener(ChannelMessageReleaseCallback releaseListener) {
    this.releaseListener = releaseListener;
  }

  public void setType(MessageType type) {
    this.type = type;
  }

  public boolean build() {

    if (header == null && buffers.size() > 0) {
      return false;
    }

    if (header != null) {
      int currentSize = 0;
      for (DataBuffer buffer : getBuffers()) {
        currentSize += buffer.getByteBuffer().remaining();
      }
//      LOG.info(String.format("Current size %d length %d", currentSize,
//          header.getLength()));
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

  public ReceivedState getReceivedState() {
    return receivedState;
  }

  public void setReceivedState(ReceivedState receivedState) {
    this.receivedState = receivedState;
  }

  public void setKeyType(MessageType keyType) {
    this.keyType = keyType;
  }

  public MessageType getKeyType() {
    return keyType;
  }

  public boolean isHeaderSent() {
    return headerSent;
  }

  public void setHeaderSent(boolean headerSent) {
    this.headerSent = headerSent;
  }

  public boolean isPartial() {
    return isPartial;
  }

  public void setPartial(boolean partial) {
    isPartial = partial;
  }
}
