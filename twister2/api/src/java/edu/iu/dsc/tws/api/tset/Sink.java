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
package edu.iu.dsc.tws.api.tset;

import java.io.Serializable;

import edu.iu.dsc.tws.dataset.DataPartition;

/**
 * Add a value at the end of the graph
 *
 * @param <T> type of sink
 */
public interface Sink<T> extends TFunction, Serializable {
  /**
   * Add a value
   *
   * @param value the value to add
   * @return true if success
   */
  boolean add(T value);

  /**
   * Call this at the end
   */
  default void close() {
  }

  /**
   * Return the data partition associated with this task instance
   */
  default DataPartition<T> get() {
    throw new UnsupportedOperationException("Get is not supported for this task, please"
        + "override the get method in the Sink interface to add functionality");
  }
}
