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
package edu.iu.dsc.tws.api.comms.packing.types.primitive;

import java.nio.ByteBuffer;

import edu.iu.dsc.tws.api.comms.messaging.types.MessageType;
import edu.iu.dsc.tws.api.comms.packing.DataBuffer;
import edu.iu.dsc.tws.api.comms.packing.DataPacker;
import edu.iu.dsc.tws.api.comms.packing.ObjectBuilder;
import edu.iu.dsc.tws.api.comms.packing.PackerStore;

@SuppressWarnings("ReturnValueIgnored")
public interface PrimitivePacker<T> extends DataPacker<T, T> {

  MessageType<T, T> getMessageType();

  /**
   * This method should put data to byteBuffer and update the position of buffer
   */
  ByteBuffer addToBuffer(ByteBuffer byteBuffer, T data);

  /**
   * This method should insert to byteBuffer's index. Position shouldn't be affected
   */
  ByteBuffer addToBuffer(ByteBuffer byteBuffer, int index, T data);

  /**
   * Read a value from offset. Buffer's position shouldn't be affected
   */
  T getFromBuffer(ByteBuffer byteBuffer, int offset);

  /**
   * Read a value from buffer. Buffer's position should be updated
   */
  T getFromBuffer(ByteBuffer byteBuffer);

  @Override
  default void writeDataToBuffer(T data, PackerStore packerStore, int alreadyCopied,
                                 int leftToCopy, int spaceLeft, ByteBuffer targetBuffer) {
    int unitDataSize = this.getMessageType().getUnitSizeInBytes();
    if (spaceLeft > unitDataSize) {
      this.addToBuffer(targetBuffer, data);
    }
  }

  @Override
  default int determineLength(T data, PackerStore store) {
    return this.getMessageType().getUnitSizeInBytes();
  }

  @SuppressWarnings("unchecked")
  @Override
  default int readDataFromBuffer(ObjectBuilder objectBuilder,
                                 int currentBufferLocation, DataBuffer dataBuffer) {
    ByteBuffer byteBuffer = dataBuffer.getByteBuffer();
    int remaining = dataBuffer.getSize() - currentBufferLocation;
    if (remaining >= this.getMessageType().getUnitSizeInBytes()) {
      T val = this.getFromBuffer(byteBuffer, currentBufferLocation);
      objectBuilder.setFinalObject(val);
      return this.getMessageType().getUnitSizeInBytes();
    } else {
      return 0;
    }
  }

  @Override
  default byte[] packToByteArray(T data) {
    byte[] byteArray = new byte[this.getMessageType().getDataSizeInBytes(data)];
    ByteBuffer wrapper = ByteBuffer.wrap(byteArray);
    this.addToBuffer(wrapper, data);
    return byteArray;
  }

  @Override
  default ByteBuffer packToByteBuffer(ByteBuffer byteBuffer, T data) {
    return this.addToBuffer(byteBuffer, data);
  }

  @Override
  default ByteBuffer packToByteBuffer(ByteBuffer byteBuffer, int index, T data) {
    return this.addToBuffer(byteBuffer, index, data);
  }

  @Override
  default T wrapperForByteLength(int byteLength) {
    return null;
  }

  @Override
  default boolean isHeaderRequired() {
    return false;
  }

  @Override
  default T unpackFromBuffer(ByteBuffer byteBuffer, int bufferOffset, int byteLength) {
    return this.getFromBuffer(byteBuffer, bufferOffset);
  }

  @Override
  default T unpackFromBuffer(ByteBuffer byteBuffer, int byteLength) {
    return this.getFromBuffer(byteBuffer);
  }
}
