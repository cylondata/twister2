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

/**
 * A interface for reading a a continous data set stored in eighther memory or file system
 * @param <T>
 */
public interface ControlledReader<T> extends RestorableIterator<T>,
    Comparable<ControlledReader<T>> {
  /**
   * Get the next key
   * @return return the next key
   */
  Object nextKey();

  /**
   * Open the reader
   */
  void open();

  /**
   * Release resources associated with this reader
   */
  void releaseResources();
}
