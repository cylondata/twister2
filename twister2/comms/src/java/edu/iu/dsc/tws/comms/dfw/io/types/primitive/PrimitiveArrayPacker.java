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
import edu.iu.dsc.tws.comms.api.ObjectBuilder;
import edu.iu.dsc.tws.comms.api.PackerStore;
import edu.iu.dsc.tws.comms.dfw.DataBuffer;

public interface PrimitiveArrayPacker<A> extends DataPacker<A, A> {

  MessageType<A, A> getMessageType();

  /**
   * Adds data[index] to the byteBuffer, position will be updated
   */
  ByteBuffer addToBuffer(ByteBuffer byteBuffer, A data, int index);

  /**
   * Adds data[index] to the byteBuffer, position will not be updated
   */
  ByteBuffer addToBuffer(ByteBuffer byteBuffer, int offset, A data, int index);

  /**
   * Read data from buffer and set to the array. Buffer position shouldn't be affected
   *
   * @param byteBuffer {@link ByteBuffer} instance
   * @param offset offset of byteBuffer
   * @param array destination array
   * @param index index of array to update
   */
  void readFromBufferAndSet(ByteBuffer byteBuffer, int offset, A array, int index);

  /**
   * Read data from buffer and set to the array. Buffer position should be updated.
   *
   * @param byteBuffer {@link ByteBuffer} instance
   * @param array destination array
   * @param index index of array to update
   */
  void readFromBufferAndSet(ByteBuffer byteBuffer, A array, int index);

  A wrapperForLength(int length);

  @Override
  default A wrapperForByteLength(int byteLength) {
    return this.wrapperForLength(byteLength / this.getMessageType().getUnitSizeInBytes());
  }

  @Override
  default int determineLength(A data, PackerStore store) {
    return this.getMessageType().getDataSizeInBytes(data);
  }

  @Override
  default void writeDataToBuffer(A data, PackerStore packerStore, int alreadyCopied,
                                 int leftToCopy, int spaceLeft, ByteBuffer targetBuffer) {
    int unitSize = this.getMessageType().getUnitSizeInBytes();
    int elementsCopied = alreadyCopied / unitSize;
    int elementsLeft = leftToCopy / unitSize;

    int spacePermitsFor = spaceLeft / unitSize;

    int willCopy = Math.min(spacePermitsFor, elementsLeft);

    for (int i = 0; i < willCopy; i++) {
      this.addToBuffer(targetBuffer, data, i + elementsCopied);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  default int readDataFromBuffer(ObjectBuilder objectBuilder,
                                 int currentBufferLocation, DataBuffer dataBuffer) {
    int totalDataLength = objectBuilder.getTotalSize();
    int startIndex = objectBuilder.getCompletedSize();
    int unitSize = this.getMessageType().getUnitSizeInBytes();
    startIndex = startIndex / unitSize;
    A val = (A) objectBuilder.getPartialDataHolder();

    //deserializing
    int noOfElements = totalDataLength / unitSize;
    int bufferPosition = currentBufferLocation;
    int bytesRead = 0;

    final ByteBuffer byteBuffer = dataBuffer.getByteBuffer();
    int size = dataBuffer.getSize();
    for (int i = startIndex; i < noOfElements; i++) {
      int remaining = size - bufferPosition;
      if (remaining >= unitSize) {
        this.readFromBufferAndSet(byteBuffer, bufferPosition, val, i);
        bytesRead += unitSize;
        bufferPosition += unitSize;
      } else {
        break;
      }
    }
    if (totalDataLength == bytesRead + startIndex * unitSize) {
      objectBuilder.setFinalObject(val);
    }
    return bytesRead;
  }

  default byte[] packToByteArray(A data) {
    byte[] byteArray = new byte[this.getMessageType().getDataSizeInBytes(data)];
    ByteBuffer wrapper = ByteBuffer.wrap(byteArray);
    this.packToByteBuffer(wrapper, data);
    return byteArray;
  }

  @Override
  default ByteBuffer packToByteBuffer(ByteBuffer byteBuffer, A data) {
    for (int i = 0; i < Array.getLength(data); i++) {
      this.addToBuffer(byteBuffer, data, i);
    }
    return byteBuffer;
  }

  @Override
  default ByteBuffer packToByteBuffer(ByteBuffer byteBuffer, int offset, A data) {
    for (int i = 0; i < Array.getLength(data); i++) {
      this.addToBuffer(byteBuffer, offset + i * getMessageType().getUnitSizeInBytes(),
          data, i);
    }
    return byteBuffer;
  }

  @Override
  default boolean isHeaderRequired() {
    return true;
  }

  @Override
  default A unpackFromBuffer(ByteBuffer byteBuffer, int bufferOffset, int byteLength) {
    A array = this.wrapperForByteLength(byteLength);
    int noOfElements = byteLength / this.getMessageType().getUnitSizeInBytes();
    int unitSize = this.getMessageType().getUnitSizeInBytes();
    int initialBufferPosition = byteBuffer.position();
    for (int i = 0; i < noOfElements; i++) {
      this.readFromBufferAndSet(byteBuffer,
          bufferOffset + initialBufferPosition + i * unitSize, array, i);
    }
    return array;
  }

  @Override
  default A unpackFromBuffer(ByteBuffer byteBuffer, int byteLength) {
    A array = this.wrapperForByteLength(byteLength);
    int noOfElements = byteLength / this.getMessageType().getUnitSizeInBytes();
    for (int i = 0; i < noOfElements; i++) {
      this.readFromBufferAndSet(byteBuffer, array, i);
    }
    return array;
  }
}
