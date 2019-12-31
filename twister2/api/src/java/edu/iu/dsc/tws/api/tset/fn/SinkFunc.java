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

package edu.iu.dsc.tws.api.tset.fn;

import edu.iu.dsc.tws.api.dataset.DataPartition;
import edu.iu.dsc.tws.api.dataset.EmptyDataPartition;

/**
 * Adds data sink at the end of the graph. {@link SinkFunc} would receive data from an edge of
 * the data flow graph.
 *
 * @param <T> data type
 */
public interface SinkFunc<T> extends TFunction<T, T> {

  /**
   * Every time an edge produces a value, this method is called.
   *
   * @param value the value to add
   * @return true if success
   */
  boolean add(T value);

  /**
   * Return the data partition associated with this task instance. Specifically to be used for
   * caching functionality.
   *
   * @return data partition - type remains unknown as the output of sunk data can be a
   * different type than T. Example, caching gatherTlink,{@literal T -> Tuple<int, T>} but the output would
   * be T
   */
  default DataPartition<?> get() {
    return EmptyDataPartition.getInstance();
  }
}
