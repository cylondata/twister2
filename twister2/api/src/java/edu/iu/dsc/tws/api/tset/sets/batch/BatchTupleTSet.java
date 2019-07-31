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

package edu.iu.dsc.tws.api.tset.sets.batch;

import java.util.Iterator;

import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.tset.fn.PartitionFunc;
import edu.iu.dsc.tws.api.tset.fn.ReduceFunc;
import edu.iu.dsc.tws.api.tset.link.TLink;
import edu.iu.dsc.tws.api.tset.sets.BuildableTSet;

/**
 * Twister data set.
 *
 * @param <T> type of the data set
 */
public interface BatchTupleTSet<K, V, T> extends BuildableTSet {
  /**
   * Name of the tset
   */
  BatchTupleTSet<K, V, T> setName(String name);

  /**
   * Gather by key
   *
   * @return this TSet
   */
  TLink<Iterator<Tuple<K, Iterator<V>>>, Tuple<K, Iterator<V>>> keyedGather();

  /**
   * Reduce by key
   *
   * @param reduceFn the reduce function
   * @return this set
   */
  TLink<Iterator<Tuple<K, V>>, Tuple<K, V>> keyedReduce(ReduceFunc<V> reduceFn);

  /**
   * Partition by key
   *
   * @param partitionFn partition function
   * @return this set
   */
  TLink<Iterator<Tuple<K, V>>, Tuple<K, V>> keyedPartition(PartitionFunc<K> partitionFn);

}
