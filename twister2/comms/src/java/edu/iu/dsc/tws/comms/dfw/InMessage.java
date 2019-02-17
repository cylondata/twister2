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
import java.util.logging.Logger;

import edu.iu.dsc.tws.comms.api.MessageHeader;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.dfw.io.Tuple;

public class InMessage {
  private static final Logger LOG = Logger.getLogger(InMessage.class.getName());

  public enum ReceivedState {
    INIT,
    BUILDING,
    BUILT,
    RECEIVE,
    DONE,
  }

  /**
   * The channels built
   */
  private Queue<ChannelMessage> builtMessages = new LinkedBlockingQueue<>();

  /**
   * The buffers added to this message
   */
  private Queue<DataBuffer> buffers = new LinkedBlockingQueue<>();

  /**
   * The overflow buffers created
   */
  private Queue<DataBuffer> overFlowBuffers = new LinkedBlockingQueue<>();

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
   * The deserialized data
   */
  private Object deserializedData;

  /**
   * Deserialized key
   */
  private Object deserializedKey;

  /**
   * The object that is being built
   */
  private Object deserializingObject;

  /**
   * The key that is being built
   */
  private Object deserializingKey;

  /**
   * Number of buffers added
   */
  private int addedBuffers = 0;

  // the amount of data we have seen for current object
  private int bufferPreviousReadForObject = 0;

  // keep track of the current object length
  private int bufferCurrentObjectLength = 0;

  // the objects we have in buffers so far, this doesn't mean we have un-packed them
  private int bufferSeenObjects = 0;

  /**
   * The length of the total object
   */
  private int unPkCurrentObjectLength = 0;

  /**
   * The length of the key unpacked
   */
  private int unPkCurrentKeyLength = -1;

  /**
   * The number of objects unpacked
   */
  private int unPkNumberObjects = 0;

  /**
   * Number of buffers we have unpacked
   */
  private int unPkBuffers = 0;

  /**
   * The current index of unpack
   */
  private int unPkCurrentIndex = 0;

  /**
   * Weather we are reading the key
   */
  private boolean readingKey = true;

  /**
   * Weather this is a keyed message
   */
  private boolean keyed;


  /**
   * Received state
   */
  private ReceivedState receivedState;

  public InMessage(int originatingId, MessageType messageType,
                   ChannelMessageReleaseCallback releaseListener,
                   MessageHeader header) {
    this.releaseListener = releaseListener;
    this.originatingId = originatingId;
    this.complete = false;
    this.dataType = messageType;
    this.receivedState = ReceivedState.INIT;
    this.header = header;
    if (header.getNumberTuples() > 0) {
      deserializedData = new ArrayList<>();
    }
  }

  public void setDataType(MessageType dataType) {
    this.dataType = dataType;
  }

  public MessageType getDataType() {
    return dataType;
  }

  public void setKeyType(MessageType keyType) {
    this.keyType = keyType;
  }

  public MessageType getKeyType() {
    keyed = true;
    return keyType;
  }

  public MessageHeader getHeader() {
    return header;
  }

  /**
   * Add a buffer and calculate weather we have seen all the buffers for an object
   * @param buffer buffer
   * @return true if all the buffers for a message is received
   */
  public boolean addBufferAndCalculate(DataBuffer buffer) {
    addedBuffers++;

    int expectedObjects = header.getNumberTuples();
    int remaining = buffer.getSize();
    int currentLocation = 0;

    // eventough we are not expecting buffers, header came in this buffer
    // so we need to add it
    if (expectedObjects == 0) {
      buffers.add(buffer);
      complete = true;
      return true;
    }

    // if this is the first buffer or, we haven't read the current object length
    if (addedBuffers == 1) {
      currentLocation = 16;
      bufferCurrentObjectLength = buffer.getByteBuffer().getInt(currentLocation);
      remaining = remaining - Integer.BYTES - 16;
      currentLocation += Integer.BYTES;
    } else if (bufferCurrentObjectLength == -1) {
      bufferCurrentObjectLength = buffer.getByteBuffer().getInt(0);
      remaining = remaining - Integer.BYTES;
      currentLocation += Integer.BYTES;
    }

    while (remaining > 0) {
      // need to read this much
      int moreToReadForCurrentObject = bufferCurrentObjectLength - bufferPreviousReadForObject;
      // amount of data in the buffer
      if (moreToReadForCurrentObject <= remaining) {
        bufferSeenObjects++;
        bufferPreviousReadForObject = 0;
        remaining = remaining - moreToReadForCurrentObject;
        currentLocation += moreToReadForCurrentObject;
      } else {
        bufferPreviousReadForObject += remaining;
        break;
      }

      // if we have seen all, lets break
      if (Math.abs(expectedObjects) == bufferSeenObjects) {
        complete = true;
        break;
      }

      // we can read another object
      if (remaining >= Integer.BYTES) {
        try {
          bufferCurrentObjectLength = buffer.getByteBuffer().getInt(currentLocation);
          bufferPreviousReadForObject = 0;
          currentLocation += Integer.BYTES;
          remaining = remaining - Integer.BYTES;
        } catch (IndexOutOfBoundsException e) {
          LOG.info(String.format("Exception remaining %d size %d currentLoc %d", remaining,
              buffer.getSize(), currentLocation));
          throw e;
        }
      } else {
        // we need to break, we set the length to -1 because we need to read the length
        // in next buffer
        bufferCurrentObjectLength = -1;
        break;
      }
    }
    buffers.add(buffer);
    return complete;
  }

  @SuppressWarnings("unchecked")
  public void addCurrentKeyedObject() {
    if (header.getNumberTuples() == -1) {
      deserializedData = new Tuple(deserializedKey, deserializingObject, keyType, dataType);
    } else {
      ((List<Object>) deserializedData).add(new Tuple(deserializedKey, deserializingObject,
          keyType, dataType));
    }
    unPkNumberObjects++;
    deserializingObject = null;
  }

  @SuppressWarnings("unchecked")
  public void addCurrentObject() {
    if (header.getNumberTuples() == -1) {
      deserializedData = deserializingObject;
    } else {
      ((List<Object>) deserializedData).add(deserializingObject);
    }
    unPkNumberObjects++;
    deserializingObject = null;
  }

  public void addBuiltMessage(ChannelMessage channelMessage) {
    builtMessages.add(channelMessage);
  }

  public ChannelMessageReleaseCallback getReleaseListener() {
    return releaseListener;
  }

  public int getOriginatingId() {
    return originatingId;
  }

  public Queue<DataBuffer> getBuffers() {
    return buffers;
  }

  public ReceivedState getReceivedState() {
    return receivedState;
  }

  public void setReceivedState(ReceivedState receivedState) {
    this.receivedState = receivedState;
  }

  public Queue<ChannelMessage> getBuiltMessages() {
    return builtMessages;
  }

  public Object getDeserializedData() {
    return deserializedData;
  }

  public Object getDeserializingObject() {
    return deserializingObject;
  }

  public void setDeserializingObject(Object deserializingObject) {
    this.deserializingObject = deserializingObject;
  }

  public Object getDeserializingKey() {
    return deserializingKey;
  }

  public void setDeserializingKey(Object deserializingKey) {
    this.deserializingKey = deserializingKey;
  }

  public void addOverFlowBuffer(DataBuffer buffer) {
    overFlowBuffers.offer(buffer);
  }

  public int getBufferSeenObjects() {
    return bufferSeenObjects;
  }

  public int getUnPkCurrentObjectLength() {
    return unPkCurrentObjectLength;
  }

  public void setUnPkCurrentObjectLength(int unPkCurrentObjectLength) {
    this.unPkCurrentObjectLength = unPkCurrentObjectLength;
  }

  public int getUnPkCurrentKeyLength() {
    return unPkCurrentKeyLength;
  }

  public void setUnPkCurrentKeyLength(int unPkCurrentKeyLength) {
    this.unPkCurrentKeyLength = unPkCurrentKeyLength;
  }

  public int getUnPkNumberObjects() {
    return unPkNumberObjects;
  }

  public int getUnPkBuffers() {
    return unPkBuffers;
  }

  public void incrementUnPkBuffers() {
    unPkBuffers++;
  }

  public int getUnPkCurrentBytes() {
    return unPkCurrentIndex;
  }

  public int addUnPkCurrentBytes(int bytes) {
    unPkCurrentIndex = unPkCurrentIndex + bytes;
    return unPkCurrentIndex;
  }

  public void setUnPkCurrentBytes(int unPkCurrentBytes) {
    this.unPkCurrentIndex = unPkCurrentBytes;
  }

  public void resetUnPk() {
    unPkCurrentObjectLength = -1;
    unPkCurrentKeyLength = -1;
    unPkCurrentIndex = 0;
    deserializingObject = null;
  }

  public boolean isKeyed() {
    return keyed;
  }

  public boolean isReadingKey() {
    return readingKey;
  }

  public void setReadingKey(boolean readingKey) {
    this.readingKey = readingKey;
  }

  public void resetUnPkKey() {
    unPkCurrentIndex = 0;
    deserializedKey = deserializingKey;
    deserializingKey = null;
    readingKey = false;
  }
}
