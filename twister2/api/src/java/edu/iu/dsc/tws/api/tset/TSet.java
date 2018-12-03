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
   * Map
   *
   * @param mapFn
   * @param <P>
   * @return
   */
  <P> TSet<P> map(MapFunction<T, P> mapFn);

  /**
   * Flatmap
   *
   * @param mapFn
   * @param <P>
   * @return
   */
  <P> TSet<P> flatMap(FlatMapFunction<T, P> mapFn);

  /**
   * Map
   *
   * @param mapFn
   * @param <P>
   * @return
   */
  <P> TSet<P> map(IterableMapFunction<T, P> mapFn);

  /**
   * Flatmap
   *
   * @param mapFn
   * @param <P>
   * @return
   */
  <P> TSet<P> flatMap(IterableFlatMapFunction<T, P> mapFn);

  /**
   * Reduce
   *
   * @param reduceFn
   * @return
   */
  TSet<T> reduce(ReduceFunction<T> reduceFn);

  /**
   * Reduce
   *
   * @param reduceFn
   * @return
   */
  TSet<T> allReduce(ReduceFunction<T> reduceFn);

  /**
   * Partition the data and reduce based on the reduce function
   *
   * @param reduceFn
   * @param partitioner
   * @return
   */
  TSet<T> reduceByKey(ReduceFunction<T> reduceFn, PartitionFunction<T> partitioner);

  /**
   * Partition the data according the to partition function
   *
   * @param partitionFn
   * @return
   */
  TSet<T> partition(PartitionFunction<T> partitionFn);

  /**
   * Gather the set of values into a single partition
   *
   * @return
   */
  TSet<T> gather();

  /**
   * Partition the data according to partitioner and gather into different
   * @param partitioner
   * @return
   */
  TSet<T> gatherByKey(PartitionFunction<T> partitioner);

  /**
   * Gather the set of values into a single partition
   *
   * @return
   */
  TSet<T> allGather();

  /**
   * Add a sink
   *
   * @param sink
   */
  void sink(Sink<T> sink);

  /**
   * Build this tset
   */
  void build();
}
