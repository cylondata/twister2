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
package edu.iu.dsc.tws.comms.dfw.io.types.primitive;

import java.lang.reflect.Array;
import java.nio.ByteBuffer;

import edu.iu.dsc.tws.comms.api.DataPacker;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.dfw.DataBuffer;
import edu.iu.dsc.tws.comms.dfw.InMessage;
import edu.iu.dsc.tws.comms.dfw.io.SerializeState;

public interface PrimitiveArrayPacker<A> extends DataPacker<A> {

  MessageType<A> getMessageType();

  ByteBuffer addToBuffer(ByteBuffer byteBuffer, A data, int index);

  void readFromBufferAndSet(ByteBuffer byteBuffer, int offset, A array, int index);

  A wrapperForLength(int length);

  @Override
  default A wrapperForByteLength(int byteLength) {
    return this.wrapperForLength(byteLength / this.getMessageType().getUnitSizeInBytes());
  }

  @Override
  default int packData(A data, SerializeState state) {
    return this.getMessageType().getDataSizeInBytes(data);
  }

  @Override
  default boolean writeDataToBuffer(A data, ByteBuffer targetBuffer, SerializeState state) {
    int totalBytes = state.getTotalBytes(); // total bytes copied so far
    int remainingCapacity = targetBuffer.remaining();
    int bytesCopied = state.getBytesCopied(); // total bytes copied for this list

    int lengthOfData = this.getMessageType().getDataSizeInBytes(data);
    int unitSize = this.getMessageType().getUnitSizeInBytes();

    int remainingToCopy = lengthOfData - bytesCopied;

    int canCopy = (remainingCapacity > remainingToCopy ? remainingToCopy
        : remainingCapacity) / unitSize; // amount that can be copied to this buffer

    // copy
    int offSet = bytesCopied / unitSize;
    for (int i = 0; i < canCopy; i++) {
      this.addToBuffer(targetBuffer, data, i + offSet);
    }

    //updating state
    totalBytes = totalBytes + canCopy * unitSize;
    // we set the total bytes copied so far
    state.setTotalBytes(totalBytes);
    // we copied everything
    if ((canCopy * unitSize) == remainingToCopy) {
      state.setData(null);
      state.setBytesCopied(0);
      return true;
    } else {
      state.setBytesCopied(canCopy * unitSize + bytesCopied);
      return false;
    }
  }

  @Override
  default int readDataFromBuffer(InMessage currentMessage, int currentLocation,
                                 DataBuffer buffer, int currentObjectLength) {
    int startIndex = currentMessage.getUnPkCurrentBytes();
    int unitSize = this.getMessageType().getUnitSizeInBytes();
    startIndex = startIndex / unitSize;
    A val = (A) currentMessage.getDeserializingObject();

    //deserializing
    int noOfElements = currentObjectLength / unitSize;
    int bufferPosition = currentLocation;
    int bytesRead = 0;
    for (int i = startIndex; i < noOfElements; i++) {
      ByteBuffer byteBuffer = buffer.getByteBuffer();
      int remaining = buffer.getSize() - bufferPosition;
      if (remaining >= unitSize) {
        this.readFromBufferAndSet(byteBuffer, bufferPosition, val, i);
        bytesRead += unitSize;
        bufferPosition += unitSize;
      } else {
        break;
      }
    }
    return bytesRead;
  }

  default byte[] toByteArray(A data) {
    byte[] byteArray = new byte[this.getMessageType().getDataSizeInBytes(data)];
    ByteBuffer wrapper = ByteBuffer.wrap(byteArray);
    for (int i = 0; i < Array.getLength(data); i++) {
      this.addToBuffer(wrapper, data, i);
    }
    return byteArray;
  }

  @Override
  default boolean isHeaderRequired() {
    return true;
  }

  @Override
  default A unpackFromBuffer(ByteBuffer byteBuffer, int byteLength) {
    A array = this.wrapperForByteLength(byteLength);
    int noOfElements = byteLength / this.getMessageType().getUnitSizeInBytes();
    int unitSize = this.getMessageType().getUnitSizeInBytes();
    for (int i = 0; i < noOfElements; i++) {
      this.readFromBufferAndSet(byteBuffer, i * unitSize, array, i);
    }
    return array;
  }
}
