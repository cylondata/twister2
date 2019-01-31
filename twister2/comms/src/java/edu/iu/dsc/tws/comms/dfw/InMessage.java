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
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import edu.iu.dsc.tws.comms.api.MessageHeader;
import edu.iu.dsc.tws.comms.api.MessageType;

public class InMessage {
  private Queue<ChannelMessage> channelMessages = new LinkedBlockingQueue<>();

  /**
   * The buffers added to this message
   */
  private Queue<DataBuffer> buffers = new LinkedBlockingQueue<>();

  /**
   * We call this to release the buffers
   */
  private ChannelMessageReleaseCallback releaseListener;

  /**
   * Keep track of the originating id, this is required to release the buffers allocated.
   */
  private int originatingId;

  /**
   * The message header
   */
  protected MessageHeader header;

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
   * Keep whether we have all the buffers added
   */
  protected boolean complete = false;

  /**
   * Message type
   */
  private MessageType dataType;

  /**
   * If a keyed message, the key being used
   */
  private MessageType keyType = MessageType.INTEGER;

  /**
   * Type of the message, weather request or send
   */
  private MessageDirection messageDirection;

  /**
   * The deserialized data
   */
  private Object deserializedData;

  /**
   * Number of objects we have read so far
   */
  private int objectsDeserialized = 0;

  /**
   * The object that is been built
   */
  private Object deserializingObject;

  /**
   * The current buffer read index
   */
  private int currentBufferIndex = 0;

  /**
   * Number of buffers added
   */
  private int addedBuffers = 0;

  // the amount of data we have seen for current object
  private int previousReadForObject = 0;

  // keep track of the current object length
  private int currentObjectLength = 0;

  // the objects we have in buffers so far
  private int seenObjects = 0;

  /**
   * The current reading buffer index, this can change from 0 > to 0 when we release the buffers
   * while we are still reading
   */
  private int currentReadingBuffer;

  public InMessage(int originatingId, MessageType messageType,
                        MessageDirection messageDirection,
                        ChannelMessageReleaseCallback releaseListener) {
    this.releaseListener = releaseListener;
    this.originatingId = originatingId;
    this.complete = false;
    this.dataType = messageType;
  }

  public void setDataType(MessageType dataType) {
    this.dataType = dataType;
  }

  public void setKeyType(MessageType keyType) {
    this.keyType = keyType;
  }

  public MessageType getKeyType() {
    return keyType;
  }

  public void setHeader(MessageHeader header) {
    this.header = header;
  }

  public MessageHeader getHeader() {
    return header;
  }

  public boolean addBufferAndCalculate(DataBuffer buffer) {
    buffers.add(buffer);
    addedBuffers++;

    int expectedObjects = header.getNumberTuples();
    int remaining = 0;
    if (addedBuffers == 1) {
      currentObjectLength = buffer.getByteBuffer().getInt(16);
      remaining = buffer.getByteBuffer().remaining() + Integer.BYTES;
    }


    while (remaining > 0) {
      // need to read this much
      int moreToReadForCurrentObject = currentObjectLength - previousReadForObject;
      // amount of data in the buffer
      if (moreToReadForCurrentObject < remaining) {
        seenObjects++;
        remaining = remaining - moreToReadForCurrentObject;
      } else {
        previousReadForObject += remaining;
        break;
      }

      // if we have seen all, lets break
      if (expectedObjects == seenObjects) {
        complete = true;
        break;
      }

      // we can read another object
      if (remaining > Integer.BYTES) {
        currentObjectLength = buffer.getByteBuffer().getInt();
        previousReadForObject = 0;
      }
    }

    return complete;
  }

  public Queue<ChannelMessage> getChannelMessages() {
    return channelMessages;
  }

  public ChannelMessage getFirstChannelMessage() {
    return channelMessages.peek();
  }

  public void addChannelMessage(ChannelMessage channelMessage) {
    channelMessages.add(channelMessage);
  }

  public int getOriginatingId() {
    return originatingId;
  }

  public List<DataBuffer> getBuffers() {
    return new ArrayList<>(buffers);
  }
}
