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

/**
 * Add a value at the end of the graph
 *
 * @param <T> input type of sink
 */
public interface SinkFunc<T> extends TFunction {
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
   * Return the data partition associated with this task instance. Specifically to be used for
   * caching functionality.
   *
   * @return data partition - type remains unknown as the output of sinked data can be a
   * different type than T. Example, caching gatherTlink, T -> Tuple<int, T> but the output would
   * be T
   */
  default DataPartition<?> get() {
    throw new UnsupportedOperationException("Get is not supported for this task, please"
        + "override the get method in the Sink interface to add functionality");
  }
}
