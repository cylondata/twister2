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
package edu.iu.dsc.tws.api.tset.link;

import edu.iu.dsc.tws.api.tset.PartitionFunction;
import edu.iu.dsc.tws.api.tset.ReduceFunction;

/**
 * Link represents the connections between data Links.
 * This would map to some form of communication patter in the communication layer
 */
public interface TLink<T> {
  /**
   * Link the parallelism for this link
   *
   * @param parallelism parallelism
   * @return this link
   */
  TLink<T> LinkParallelism(int parallelism);

  /**
   * Name of the Tlink
   */
  TLink<T> LinkName(String name);

  /**
   * Direct operation
   *
   * @return this TLink
   */
  TLink<T> direct();

  /**
   * Reduce operation on the data
   *
   * @param reduceFn the reduce function
   * @return this Link
   */
  TLink<T> reduce(ReduceFunction<T> reduceFn);

  /**
   * All reduce operation
   *
   * @param reduceFn reduce function
   * @return this Link
   */
  TLink<T> allReduce(ReduceFunction<T> reduceFn);

  /**
   * Partition the data according the to partition function
   *
   * @param partitionFn partition function
   * @return this Link
   */
  TLink<T> partition(PartitionFunction<T> partitionFn);

  /**
   * Gather the Link of values into a single partition
   *
   * @return this Link
   */
  TLink<T> gather();

  /**
   * Gather the Link of values into a single partition
   *
   * @return this Link
   */
  TLink<T> allGather();
}
