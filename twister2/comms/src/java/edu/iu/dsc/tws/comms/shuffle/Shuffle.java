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
package edu.iu.dsc.tws.comms.shuffle;

import java.util.Iterator;

/**
 * Different interfaces to go to disk
 */
public interface Shuffle {
  /**
   * Switch to reading, we cannot add after this
   */
  void switchToReading();

  /**
   * Get a read iterator
   * @return an iterator
   */
  Iterator<Object> readIterator();

  /**
   * Add an object with a key
   * @param key the object key
   * @param data the data
   * @param length length of the data
   */
  default void add(Object key, byte[] data, int length) {
    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Add object
   * @param data the data as bytes
   * @param length the length of data
   */
  default void add(byte[] data, int length) {
    throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Execute the shuffle operation. This will save the content
   */
  default void run() {
  }

  /**
   * Clean the file system
   */
  default void clean() {
  }
}
