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
   * Stores the give key value pair in the memory manager
   *
   * @param key key of the pair
   * @param value value to be stored
   * @return true of the key value pair was added, false otherwise
   */
  boolean put(byte[] key, byte[] value);

  /**
   * Stores the give key value pair in the memory manager
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
   * Get the corresponding value for the given key from the store
   */
  byte[] get(byte[] key);


}
