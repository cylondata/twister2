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

import java.nio.ByteBuffer;

import org.apache.commons.lang3.tuple.Pair;

import edu.iu.dsc.tws.comms.api.DataPacker;
import edu.iu.dsc.tws.comms.api.KeyPacker;
import edu.iu.dsc.tws.comms.api.MessageType;
import edu.iu.dsc.tws.comms.dfw.DataBuffer;
import edu.iu.dsc.tws.comms.dfw.InMessage;
import edu.iu.dsc.tws.comms.dfw.io.SerializeState;

@SuppressWarnings("ReturnValueIgnored")
public interface PrimitivePacker<T> extends KeyPacker<T>, DataPacker<T> {

  MessageType<T> getMessageType();

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
  default boolean writeKeyToBuffer(T key, ByteBuffer targetBuffer, SerializeState state) {
    int unitDataSize = this.getMessageType().getUnitSizeInBytes();
    if (targetBuffer.remaining() > unitDataSize) {
      this.addToBuffer(targetBuffer, key);
      state.setTotalBytes(state.getTotalBytes() + unitDataSize);
      state.setCurrentHeaderLength(state.getCurrentHeaderLength() + unitDataSize);
      state.setKeySize(unitDataSize);
      return true;
    }
    return false;
  }

  @Override
  default boolean writeDataToBuffer(T data, ByteBuffer targetBuffer, SerializeState state) {
    int unitDataSize = this.getMessageType().getUnitSizeInBytes();
    if (targetBuffer.remaining() > unitDataSize) {
      this.addToBuffer(targetBuffer, data);
      state.setTotalBytes(state.getTotalBytes() + unitDataSize);
      //since it's a single value.
      state.setData(null);
      state.setBytesCopied(0);
      return true;
    }
    return false;
  }

  @Override
  default Pair<Integer, Integer> getKeyLength(InMessage message,
                                              DataBuffer buffer, int location) {
    return Pair.of(this.getMessageType().getUnitSizeInBytes(), 0);
  }

  @Override
  default int packKey(T key, SerializeState state) {
    return this.packToState(key, state);
  }

  @Override
  default int packToState(T data, SerializeState state) {
    return this.getMessageType().getUnitSizeInBytes();
  }

  @Override
  default int readDataFromBuffer(InMessage currentMessage, int currentLocation,
                                 DataBuffer buffers, int currentObjectLength) {
    ByteBuffer byteBuffer = buffers.getByteBuffer();
    int remaining = buffers.getSize() - currentLocation;
    if (remaining >= this.getMessageType().getUnitSizeInBytes()) {
      T val = this.getFromBuffer(byteBuffer, currentLocation);
      currentMessage.setDeserializingKey(val);
      return this.getMessageType().getUnitSizeInBytes();
    } else {
      return 0;
    }
  }

  @Override
  default int readKeyFromBuffer(InMessage currentMessage, int currentLocation,
                                DataBuffer buffer, int currentObjectLength) {
    return this.readDataFromBuffer(
        currentMessage,
        currentLocation,
        buffer,
        currentObjectLength
    );
  }

  @Override
  default boolean isKeyHeaderRequired() {
    //will be false for primitive
    return this.isHeaderRequired();
  }

  @Override
  default T initializeUnPackKeyObject(int size) {
    //will be null for primitive
    return this.wrapperForByteLength(size);
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
