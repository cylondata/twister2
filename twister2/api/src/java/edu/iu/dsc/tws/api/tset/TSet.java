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
   * Name of the tset
   */
  TSet<T> setName(String name);

  /**
   * Apply a map function and create a map data set
   *
   * @param mapFn map function
   * @param <P> return type of function
   * @return a TSet
   */
  <P> MapTSet<P, T> map(MapFunction<T, P> mapFn);

  /**
   * Flatmap
   */
  <P> FlatMapTSet<P, T> flatMap(FlatMapFunction<T, P> mapFn);

  /**
   * Map
   */
  <P> IMapTSet<P, T> map(IterableMapFunction<T, P> mapFn);

  /**
   * Flatmap
   */
  <P> IFlatMapTSet<P, T> flatMap(IterableFlatMapFunction<T, P> mapFn);

  /**
   * Reduce
   */
  TSet<T> reduce(ReduceFunction<T> reduceFn);

  /**
   * Reduce
   */
  TSet<T> allReduce(ReduceFunction<T> reduceFn);

  /**
   * Partition the data according the to partition function
   */
  TSet<T> partition(PartitionFunction<T> partitionFn);

  /**
   * Gather the set of values into a single partition
   */
  TSet<T> gather();

  /**
   * Gather the set of values into a single partition
   */
  TSet<T> allGather();

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
   * Add a sink
   */
  void sink(Sink<T> sink);

  /**
   * Build this tset
   */
  void build();
}
