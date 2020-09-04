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
package edu.iu.dsc.tws.api.tset.sets;

import java.util.Collection;

import edu.iu.dsc.tws.api.comms.messaging.types.MessageType;
import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.tset.TBase;
import edu.iu.dsc.tws.api.tset.fn.MapFunc;
import edu.iu.dsc.tws.api.tset.fn.PartitionFunc;
import edu.iu.dsc.tws.api.tset.fn.ReduceFunc;
import edu.iu.dsc.tws.api.tset.link.TLink;
import edu.iu.dsc.tws.api.tset.schema.Schema;

/**
 * Twister data set. A {@link TSet} would abstract a Task level computation (Source/ Compute or
 * Sink) in a more user friendly API. A {@link TSet} would be followed by a {@link TLink} that
 * would expose the communication level operations performed on the computation.
 *
 * Note the extensions of this interface
 * {@link edu.iu.dsc.tws.api.tset.sets.batch.BatchTSet} and
 * {@link edu.iu.dsc.tws.api.tset.sets.streaming.StreamingTSet}. These would intimately separate
 * out the operations based on the {@link edu.iu.dsc.tws.api.compute.graph.OperationMode} of the
 * data flow graph.
 *
 *  This interface only specifies the common operations for Batch and Streaming operations.
 * @param <T> type of the data set
 */
public interface TSet<T> extends TBase {

  /**
   * Sets the name
   */
  @Override
  TSet<T> setName(String name);

  /**
   * Returns a Direct {@link TLink} that corresponds to the communication operation where the data
   * will be transferred to another TSet directly.
   *
   * @return Direct TLink
   */
  TLink<?, T> direct();

  /**
   * Returns a Reduce {@link TLink} that reduce data on to the target TSet instance (in the runtime)
   * with index 0.
   *
   * @param reduceFn Reduce function
   * @return Reduce TLink
   */
  TLink<?, T> reduce(ReduceFunc<T> reduceFn);

  /**
   * Similar to reduce, but all instances of the target {@link TSet} would receive the reduced
   * result.
   *
   * @param reduceFn Reduce function
   * @return AllReduce TLink
   */
  TLink<?, T> allReduce(ReduceFunc<T> reduceFn);

  /**
   * Returns a Partition {@link TLink} that would partition data according based on a function
   * provided. The parallelism of the target {@link TSet} can also be specified.
   *
   * @param partitionFn       Partition function
   * @param targetParallelism Target parallelism
   * @return Partition TLink
   */
  TLink<?, T> partition(PartitionFunc<T> partitionFn, int targetParallelism);

  /**
   * Same as above, but the parallelism will be preserved in the target {@link TSet}.
   *
   * @param partitionFn Partition function
   * @return Partition TLink
   */
  TLink<?, T> partition(PartitionFunc<T> partitionFn);

  /**
   * Returns a Gather {@link TLink} that would gather data to the target TSet instance with index
   * 0 (in the runtime).
   *
   * @return Gather TLink
   */
  TLink<?, T> gather();

  /**
   * Same as gather, but all the target TSet instances would receive the gathered result in the
   * runtime.
   *
   * @return AllGather TLink
   */
  TLink<?, T> allGather();

  /**
   * Creates a {@link TupleTSet} based on the {@link MapFunc} provided. This will an entry point
   * to keyed communication operations from a non-keyed {@link TSet}.
   *
   * @param <K> type of key
   * @param <V> type of value
   * @param mapToTupleFn Map function
   * @return Tuple TSet
   */
  <K, V> TupleTSet<K, V> mapToTuple(MapFunc<T, Tuple<K, V>> mapToTupleFn);

  /**
   * Returns a Replicate {@link TLink} that would clone/broadcast the data from this {@link TSet}.
   * Note that the parallelism of this {@link TSet} should be 1.
   *
   * @param replicas Replicas of data (= target TSet parallelism)
   * @return Replicate TLink
   */
  TLink<?, T> replicate(int replicas);

  /**
   * Returns a single {@link TSet} that create a union of data in both {@link TSet}s. In order for
   * this to work both TSets should be of the same type
   *
   * @param unionTSet TSet to union with
   * @return Union TSet
   */
  TSet<T> union(TSet<T> unionTSet);

  /**
   * Same as above, but accepts a {@link Collection} of {@link TSet}s.
   *
   * @param tSets a collection of TSet's to union with
   * @return Union TSet
   */
  TSet<T> union(Collection<TSet<T>> tSets);

  /**
   * Sets the data type of the {@link TSet} output. This will be used in the packers for efficient
   * SER-DE operations in the following {@link TLink}s
   *
   * @param dataType data type as a {@link MessageType}
   * @return this {@link TSet}
   */
  TSet<T> withSchema(Schema dataType);
}
