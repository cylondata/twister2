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

/**
 * Twister data set.
 *
 * @param <T> type of the data set
 */
public interface TSet<T> {
  /**
   * Set the parallelism for this set
   * @param parallelism parallelism
   * @return this set
   */
  TSet<T> setParallelism(int parallelism);

  /**
   * Name of the tset
   */
  TSet<T> setName(String name);

  /**
   * Apply a map function and create a map data set
   *
   * @param mapFn map function
   * @param <P> return type of function
   * @return this TSet
   */
  <P> MapTSet<P, T> map(MapFunction<T, P> mapFn);

  /**
   * Flatmap on the data
   *
   * @param mapFn map function
   * @param <P> output type
   * @return this set
   */
  <P> FlatMapTSet<P, T> flatMap(FlatMapFunction<T, P> mapFn);

  /**
   * Map operation on the data
   *
   * @param mapFn map function
   * @param <P> output type
   * @return this set
   */
  <P> IMapTSet<P, T> map(IterableMapFunction<T, P> mapFn);

  /**
   * Flatmap operation on the data
   *
   * @param mapFn map function
   * @param <P> output type
   * @return this set
   */
  <P> IFlatMapTSet<P, T> flatMap(IterableFlatMapFunction<T, P> mapFn);

  /**
   * Reduce operation on the data
   *
   * @param reduceFn the reduce function
   * @return this set
   */
  TSet<T> reduce(ReduceFunction<T> reduceFn);

  /**
   * All reduce operation
   *
   * @param reduceFn reduce function
   * @return this set
   */
  TSet<T> allReduce(ReduceFunction<T> reduceFn);

  /**
   * Partition the data according the to partition function
   *
   * @param partitionFn partition function
   * @return this set
   */
  TSet<T> partition(PartitionFunction<T> partitionFn);

  /**
   * Gather the set of values into a single partition
   *
   * @return this set
   */
  TSet<T> gather();

  /**
   * Gather the set of values into a single partition
   *
   * @return this set
   */
  TSet<T> allGather();

  /**
   * Select a set of values
   * @param partitionFunction partition function
   * @param selector the selector
   * @param <K> the type for partitioning
   * @return grouped set
   */
  <K> GroupedTSet<T, K> groupBy(PartitionFunction<K> partitionFunction, Selector<T, K> selector);

  /**
   * Add a sink
   *
   * @param sink sink function
   */
  void sink(Sink<T> sink);

  /**
   * Build this tset
   */
  void build();
}
