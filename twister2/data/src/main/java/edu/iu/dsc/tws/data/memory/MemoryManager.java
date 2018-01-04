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
package edu.iu.dsc.tws.data.memory;

import java.nio.ByteBuffer;

/**
 * base interface for memory managers. Memory managers are responsible of keeping data in memoory
 * for various requirements. The memory manager is also responsible of writing data to disk when
 * the memory does not have enough space
 */
public interface MemoryManager {

  /**
   * Initializes the Memory manager
   */
  boolean init();

  /**
   * Appends the given byte buffer to the current value in the store.
   * If the store does not contain the given key append works as a put operation
   * @param key key to append to
   * @param value value to be appended
   * @return true if append was performed, false otherwise
   */
  boolean append(ByteBuffer key, ByteBuffer value);

  boolean append(long key, ByteBuffer value);
  /**
   * Stores the give key value pair in the memory manager
   */
  boolean put(ByteBuffer key, ByteBuffer value);

  /**
   * Stores the give key value pair in the memory manager
   *
   * @param key key of the pair
   * @param value value to be stored
   * @return true of the key value pair was added, false otherwise
   */
  boolean put(byte[] key, byte[] value);

  /**
   * Stores the give key value pair in the memory manager
   * if the key already exists the new value will be added to the key
   */
  boolean put(byte[] key, ByteBuffer value);

  /**
   * Stores the give key value pair in the memory manager
   */
  boolean put(long key, ByteBuffer value);

  /**
   * Stores the give key value pair in the memory manager
   */
  boolean put(long key, byte[] value);

  /**
   * Get the corresponding value as ByteBuffer for the given key from the store
   */
  ByteBuffer get(ByteBuffer key);

  /**
   * Get the corresponding value as ByteBuffer for the given key from the store
   */
  ByteBuffer get(byte[] key);

  /**
   * Get the corresponding value as ByteBuffer for the given key from the store
   */
  ByteBuffer get(long key);


  /**
   * Get the corresponding value as bytes for the given key from the store
   */
  byte[] getBytes(byte[] key);

  /**
   * Get the corresponding value as bytes for the given key from the store
   */
  byte[] getBytes(long key);

  /**
   * Get the corresponding value as bytes for the given key from the store
   */
  byte[] getBytes(ByteBuffer keye);

  /**
   * checks if the given key is in the memory manager
   *
   * @param key key to be checked
   * @return true if the key is present, false otherwise
   */
  boolean containsKey(ByteBuffer key);

  /**
   * checks if the given key is in the memory manager
   *
   * @param key key to be checked
   * @return true if the key is present, false otherwise
   */
  boolean containsKey(byte[] key);

  /**
   * checks if the given key is in the memory manager
   *
   * @param key key to be checked
   * @return true if the key is present, false otherwise
   */
  boolean containsKey(long key);


}
