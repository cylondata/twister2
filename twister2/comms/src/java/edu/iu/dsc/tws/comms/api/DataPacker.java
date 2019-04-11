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
package edu.iu.dsc.tws.comms.api;

import java.nio.ByteBuffer;

import edu.iu.dsc.tws.comms.dfw.DataBuffer;
import edu.iu.dsc.tws.comms.dfw.InMessage;

/**
 * The data packer interface. An implementation class should be stateless.
 */
public interface DataPacker<D> {

  /**
   * Pack the data and return the size of the data in bytes once packed.
   * However, packing(converting to byte[]) and saving to store inside this method is optional.
   * If your have primitives that can be efficiently copied to the buffers later in
   * {@link DataPacker#writeDataToBuffer}, you may just
   * return the byte size of data from this method.
   *
   * @param data the data (can be Integer, Object etc)
   * @param store A store to temporary store the serialized data,
   * if you had to serialize data to determine length.
   * @return the size of the packed data in bytes
   */
  int determineLength(D data, PackerStore store);

  /**
   * Transfer the data to the buffer. If you have already packed data to state
   * * with {@link DataPacker#determineLength(Object, PackerStore)},
   * you may transfer data from state
   * * to targetBuffer. If not, you may directly transfer data to the targetBuffer.
   */
  void writeDataToBuffer(D data,
                         edu.iu.dsc.tws.comms.api.PackerStore packerStore,
                         int alreadyCopied,
                         int leftToCopy,
                         int spaceLeft,
                         ByteBuffer targetBuffer);

  /**
   * Read the data from the buffer
   *
   * @param currentMessage the current message
   * @param currentLocation current location
   * @param buffer buffer
   * @param currentObjectLength the current object length
   * @return the number of bytes read
   */
  int readDataFromBuffer(InMessage currentMessage, int currentLocation,
                         DataBuffer buffer, int currentObjectLength);

  byte[] packToByteArray(D data);

  /**
   * This method puts a value to the {@link ByteBuffer}.
   * The position of byteBuffer should be updated.
   */
  ByteBuffer packToByteBuffer(ByteBuffer byteBuffer, D data);

  /**
   * This method puts a value to the {@link ByteBuffer}, starting from specified
   * index of the buffer.
   * The position of the buffer shouldn't be affected
   */
  ByteBuffer packToByteBuffer(ByteBuffer byteBuffer, int offset, D data);

  /**
   * Returns an empty wrapper to hold byteLength amount of type T
   */
  D wrapperForByteLength(int byteLength);

  /**
   * Indicates whether length should be packed before the actual key
   */
  boolean isHeaderRequired();

  /**
   * This method will extract a value from buffer starting from the position specified.
   * Buffer position shouldn't be affected by this method
   *
   * @param byteBuffer {@link ByteBuffer} instance
   * @param bufferOffset position to start reading from buffer
   * @param byteLength amount of data to read
   */
  D unpackFromBuffer(ByteBuffer byteBuffer, int bufferOffset, int byteLength);

  /**
   * This method will extract a value from buffer starting from buffer's current position.
   * Buffer position should be updated.
   */
  D unpackFromBuffer(ByteBuffer byteBuffer, int byteLength);

  default D unpackFromByteArray(byte[] array) {
    return this.unpackFromBuffer(ByteBuffer.wrap(array), array.length);
  }
}

