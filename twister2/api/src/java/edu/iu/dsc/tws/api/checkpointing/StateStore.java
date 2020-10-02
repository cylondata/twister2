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

package edu.iu.dsc.tws.api.checkpointing;

import java.io.IOException;

import edu.iu.dsc.tws.api.config.Config;

/**
 * Interface for saving state to different stores
 */
public interface StateStore {

  /**
   * Initialize the store
   * @param config configuration
   * @param path path
   */
  void init(Config config, String... path);

  /**
   * Put a key and data
   * @param key key
   * @param data data
   * @throws IOException if an error occurs
   */
  void put(String key, byte[] data) throws IOException;

  /**
   * Get the bye value of the key
   * @param key key
   * @return byte array
   * @throws IOException if an error occurs
   */
  byte[] get(String key) throws IOException;

  /**
   * Check weather the specified key is in the store
   * @param key key
   * @return true if the key in store
   * @throws IOException if an error occurs
   */
  boolean contains(String key) throws IOException;
}
