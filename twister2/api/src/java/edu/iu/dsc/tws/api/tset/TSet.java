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

import edu.iu.dsc.tws.api.tset.link.AllGatherTLink;
import edu.iu.dsc.tws.api.tset.link.AllReduceTLink;
import edu.iu.dsc.tws.api.tset.link.DirectTLink;
import edu.iu.dsc.tws.api.tset.link.GatherTLink;
import edu.iu.dsc.tws.api.tset.link.PartitionTLink;
import edu.iu.dsc.tws.api.tset.link.ReduceTLink;
import edu.iu.dsc.tws.api.tset.link.ReplicateTLink;
import edu.iu.dsc.tws.api.tset.link.TLink;
import edu.iu.dsc.tws.api.tset.sets.CachedTSet;
import edu.iu.dsc.tws.api.tset.sets.GroupedTSet;

/**
 * Twister data set.
 *
 * @param <T> type of the data set
 */
public interface TSet<T> extends TBase<T> {
  /**
   * Set the parallelism for this set
   *
   * @param parallelism parallelism
   * @return this set
   */
  TSet<T> setParallelism(int parallelism);

  /**
   * Name of the tset
   */
  TSet<T> setName(String name);

  /**
   * Direct operation
   *
   * @return this TSet
   */
  DirectTLink<T> direct();

  /**
   * Reduce operation on the data
   *
   * @param reduceFn the reduce function
   * @return this set
   */
  ReduceTLink<T> reduce(ReduceFunction<T> reduceFn);

  /**
   * All reduce operation
   *
   * @param reduceFn reduce function
   * @return this set
   */
  AllReduceTLink<T> allReduce(ReduceFunction<T> reduceFn);

  /**
   * Partition the data according the to partition function
   *
   * @param partitionFn partition function
   * @return this set
   */
  PartitionTLink<T> partition(PartitionFunction<T> partitionFn);

  /**
   * Gather the set of values into a single partition
   *
   * @return this set
   */
  GatherTLink<T> gather();

  /**
   * Gather the set of values into a single partition
   *
   * @return this set
   */
  AllGatherTLink<T> allGather();

  /**
   * Select a set of values
   *
   * @param partitionFunction partition function
   * @param selector the selector
   * @param <K> the type for partitioning
   * @return grouped set
   */
  <K> GroupedTSet<T, K> groupBy(PartitionFunction<K> partitionFunction, Selector<T, K> selector);

  /**
   * Create a cloned dataset
   *
   * @return the cloned set
   */
  ReplicateTLink<T> replicate(int replications);

  /**
   * Executes TSet and saves any generated data as a in-memory data object
   *
   * @return the resulting TSet
   */
  CachedTSet<T> cache();

  /**
   * Allows users to pass in other TSets as inputs for a TSet
   * @param key the key used to store the given TSet
   * @param input the TSet to be added as an input
   * @return true if the input was added successfully or false otherwise
   */
  boolean addInput(String key, Cacheable<?> input);

}
