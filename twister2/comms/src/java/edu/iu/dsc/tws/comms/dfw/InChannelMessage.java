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

import edu.iu.dsc.tws.comms.api.MessageType;

public class InChannelMessage extends ChannelMessage {
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

  public InChannelMessage(int originatingId, MessageType messageType,
                        MessageDirection messageDirection,
                        ChannelMessageReleaseCallback releaseListener) {
    super(originatingId, messageType, messageDirection, releaseListener);
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

}
