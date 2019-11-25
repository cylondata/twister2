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
import edu.iu.dsc.tws.api.tset.fn.PartitionFunc;
import edu.iu.dsc.tws.api.tset.fn.ReduceFunc;
import edu.iu.dsc.tws.api.tset.link.batch.BatchTLink;
import edu.iu.dsc.tws.api.tset.sets.AcceptingData;
import edu.iu.dsc.tws.api.tset.sets.StorableTBase;
import edu.iu.dsc.tws.api.tset.sets.StoringData;
import edu.iu.dsc.tws.api.tset.sets.TupleTSet;

/**
 * Batch extension of {@link TupleTSet} interface with {@link AcceptingData} and
 * {@link StoringData} capabilities.
 *
 * @param <K>
 * @param <V>
 */
public interface BatchTupleTSet<K, V> extends TupleTSet<K, V>,
    AcceptingData<Tuple<K, V>>, StoringData<Tuple<K, V>> {

  /**
   * Name of the tset
   */
  @Override
  BatchTupleTSet<K, V> setName(String name);

  /**
   * Partitions data using a {@link PartitionFunc} based on keys
   *
   * @param partitionFn partition function
   * @return Keyed Partition TLink
   */
  @Override
  BatchTLink<Iterator<Tuple<K, V>>, Tuple<K, V>> keyedPartition(PartitionFunc<K> partitionFn);

  /**
   * Direct/pipe communication
   *
   * @return Keyed Direct TLink
   */
  @Override
  BatchTLink<Iterator<Tuple<K, V>>, Tuple<K, V>> keyedDirect();

  /**
   * Gathers data by key for {@link BatchTupleTSet}s
   *
   * @return Keyed Gather TLink
   */
  BatchTLink<Iterator<Tuple<K, Iterator<V>>>, Tuple<K, Iterator<V>>> keyedGather();

  /**
   * Gathers data by key for {@link BatchTupleTSet}s
   *
   * @param partitionFn partition function to partition data based on key
   * @return Keyed Gather TLink
   */
  BatchTLink<Iterator<Tuple<K, Iterator<V>>>, Tuple<K, Iterator<V>>> keyedGather(
      PartitionFunc<K> partitionFn);

  /**
   * Gathers data by key for {@link BatchTupleTSet}s
   *
   * @param partitionFn partition function to partition data based on key
   * @param comparator custom key comparator
   * @return Keyed Gather TLink
   */
  BatchTLink<Iterator<Tuple<K, Iterator<V>>>, Tuple<K, Iterator<V>>> keyedGather(
      PartitionFunc<K> partitionFn, Comparator<K> comparator);

  /**
   * Gathers data by key for {@link BatchTupleTSet}s without grouping data by keys
   *
   * @return this TSet
   */
  BatchTLink<Iterator<Tuple<K, V>>, Tuple<K, V>> keyedGatherUngrouped();

  /**
   * Gathers data by key for {@link BatchTupleTSet}s without grouping data by keys
   *
   * @param partitionFn partition function to partition data based on key
   * @return Keyed Gather TLink
   */
  BatchTLink<Iterator<Tuple<K, V>>, Tuple<K, V>> keyedGatherUngrouped(PartitionFunc<K> partitionFn);

  /**
   * Gathers data by key for {@link BatchTupleTSet}s without grouping data by keys
   *
   * @param partitionFn partition function to partition data based on key
   * @param comparator custom key comparator
   * @return Keyed Gather TLink
   */
  BatchTLink<Iterator<Tuple<K, V>>, Tuple<K, V>> keyedGatherUngrouped(PartitionFunc<K> partitionFn,
                                                                      Comparator<K> comparator);

  /**
   * Reduces data by key for {@link BatchTupleTSet}s
   *
   * @param reduceFn the reduce function
   * @return Keyed Reduce TLink
   */
  BatchTLink<Iterator<Tuple<K, V>>, Tuple<K, V>> keyedReduce(ReduceFunc<V> reduceFn);

  /**
   * Joins with another {@link BatchTupleTSet}. Note that this TSet will be considered the left
   * TSet
   *
   * @param rightTSet     right tset
   * @param type          {@link edu.iu.dsc.tws.api.comms.CommunicationContext.JoinType}
   * @param keyComparator key comparator
   * @param <VR>          value type of the right tset
   * @return Joined TLink
   */
  <VR> BatchTLink<Iterator<JoinedTuple<K, V, VR>>, JoinedTuple<K, V, VR>>
      join(BatchTupleTSet<K, VR> rightTSet, CommunicationContext.JoinType type,
           Comparator<K> keyComparator);

  /**
   * Joins with another {@link BatchTupleTSet}. Note that this TSet will be considered the left
   * TSet
   *
   * @param rightTSet     right tset
   * @param type          {@link edu.iu.dsc.tws.api.comms.CommunicationContext.JoinType}
   * @param keyComparator key comparator
   * @param partitioner   partitioner for keys
   * @param <VR>          value type of the right tset
   * @return Joined TLink
   */
  <VR> BatchTLink<Iterator<JoinedTuple<K, V, VR>>, JoinedTuple<K, V, VR>>
      join(BatchTupleTSet<K, VR> rightTSet, CommunicationContext.JoinType type,
           Comparator<K> keyComparator, TaskPartitioner<K> partitioner);

  /**
   * Adds inputs to {@link BatchTupleTSet}s
   *
   * @param key   identifier for the input
   * @param input input tset
   * @return this tset
   */
  BatchTupleTSet<K, V> addInput(String key, StorableTBase<?> input);
}
