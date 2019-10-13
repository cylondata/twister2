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

import edu.iu.dsc.tws.api.compute.IFunction;

/**
 * Reduce function
 */
public interface ReduceFunc<T> extends TFunction<T, T>, IFunction<T> {
  /**
   * Reduce the t1 and t2 into a single value
   *
   * @param t1 input one
   * @param t2 input two
   * @return the reduced object
   */
  T reduce(T t1, T t2);

  @Override
  default T onMessage(T object1, T object2) {
    return reduce(object1, object2);
  }
}
