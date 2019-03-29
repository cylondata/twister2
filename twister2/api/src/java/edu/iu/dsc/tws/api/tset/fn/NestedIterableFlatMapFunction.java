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

import edu.iu.dsc.tws.api.tset.Collector;
import edu.iu.dsc.tws.api.tset.TFunction;
import edu.iu.dsc.tws.comms.dfw.io.Tuple;

/**
 * Iterable Keyed flat map function
 *
 * @param <K> key type
 * @param <V> value type
 * @param <O> return type
 */
public interface NestedIterableFlatMapFunction<K, V, O> extends TFunction {
  /**
   * Map with an iterable input, Values in the Iterator are {@link Tuple}
   * each tuple has a Key and a Value, the Value is again an Iterator of type V
   *
   * @param t input as an iteration
   * @param collector collects output
   */
  void flatMap(Iterable<Tuple<K, Iterable<V>>> t, Collector<O> collector);
}
