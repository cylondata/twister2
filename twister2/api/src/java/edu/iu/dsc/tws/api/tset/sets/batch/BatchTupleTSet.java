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

import java.util.Comparator;
import java.util.Iterator;

import edu.iu.dsc.tws.api.comms.CommunicationContext;
import edu.iu.dsc.tws.api.comms.structs.JoinedTuple;
import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.compute.TaskPartitioner;
import edu.iu.dsc.tws.api.tset.Storable;
import edu.iu.dsc.tws.api.tset.fn.PartitionFunc;
import edu.iu.dsc.tws.api.tset.fn.ReduceFunc;
import edu.iu.dsc.tws.api.tset.link.batch.BatchTLink;
import edu.iu.dsc.tws.api.tset.sets.TupleTSet;

public interface BatchTupleTSet<K, V> extends TupleTSet<K, V> {
  /**
   * Name of the tset
   */
  @Override
  BatchTupleTSet<K, V> setName(String name);

  /**
   * Partition by key
   *
   * @param partitionFn partition function
   * @return this set
   */
  @Override
  BatchTLink<Iterator<Tuple<K, V>>, Tuple<K, V>> keyedPartition(PartitionFunc<K> partitionFn);

  /**
   * Gather by key
   *
   * @return this TSet
   */
  BatchTLink<Iterator<Tuple<K, Iterator<V>>>, Tuple<K, Iterator<V>>> keyedGather();

  /**
   * Gather by key
   *
   * @return this TSet
   */
  BatchTLink<Iterator<Tuple<K, Iterator<V>>>, Tuple<K, Iterator<V>>> keyedGather(
      PartitionFunc<K> partitionFn);

  /**
   * Gather by key. Sort the records with the key according to the comparator
   *
   * @return this TSet
   */
  BatchTLink<Iterator<Tuple<K, Iterator<V>>>, Tuple<K, Iterator<V>>>
      keyedGather(PartitionFunc<K> partitionFn, Comparator<K> comparator);

  /**
   * Reduce by key
   *
   * @param reduceFn the reduce function
   * @return this set
   */
  BatchTLink<Iterator<Tuple<K, V>>, Tuple<K, V>> keyedReduce(ReduceFunc<V> reduceFn);

  <VR> BatchTLink<Iterator<JoinedTuple<K, V, VR>>, JoinedTuple<K, V, VR>>
      join(BatchTupleTSet<K, VR> rightTSet, CommunicationContext.JoinType type,
           Comparator<K> keyComparator);

  <VR> BatchTLink<Iterator<JoinedTuple<K, V, VR>>, JoinedTuple<K, V, VR>>
      join(BatchTupleTSet<K, VR> rightTSet, CommunicationContext.JoinType type,
           Comparator<K> keyComparator, TaskPartitioner<K> partitioner);

  BatchTupleTSet<K, V> addInput(String key, Storable<?> input);

  BatchTupleTSet<K, V> cache();
}
