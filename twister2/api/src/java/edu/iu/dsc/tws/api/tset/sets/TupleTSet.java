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

package edu.iu.dsc.tws.api.tset.sets;

import edu.iu.dsc.tws.api.tset.fn.PartitionFunction;
import edu.iu.dsc.tws.api.tset.fn.ReduceFunction;
import edu.iu.dsc.tws.api.tset.link.TLink;

/**
 * Twister data set.
 *
 * @param <T> type of the data set
 */
public interface TupleTSet<K, V, T> extends BuildableTSet {
  /**
   * Name of the tset
   */
  TupleTSet<K, V, T> setName(String name);

  /**
   * Gather by key
   *
   * @return this TSet
   */
  TLink<?, ?> keyedGather();

  /**
   * Reduce by key
   *
   * @param reduceFn the reduce function
   * @return this set
   */
  TLink<?, ?> keyedReduce(ReduceFunction<V> reduceFn);

  /**
   * Partition by key
   *
   * @param partitionFn partition function
   * @return this set
   */
  TLink<?, ?> keyedPartition(PartitionFunction<K> partitionFn);

}
