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

import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.tset.TBase;
import edu.iu.dsc.tws.api.tset.fn.ApplyFunc;
import edu.iu.dsc.tws.api.tset.fn.ComputeCollectorFunc;
import edu.iu.dsc.tws.api.tset.fn.ComputeFunc;
import edu.iu.dsc.tws.api.tset.fn.FlatMapFunc;
import edu.iu.dsc.tws.api.tset.fn.MapFunc;
import edu.iu.dsc.tws.api.tset.fn.SinkFunc;
import edu.iu.dsc.tws.api.tset.sets.TSet;
import edu.iu.dsc.tws.api.tset.sets.TupleTSet;

/**
 * Link represents the connections between {@link TSet}s and {@link TupleTSet}s. These would map to
 * some form of communication operations in the communication layer.
 *
 * Communication layer outputs three variations/types of data outputs (corresponds to T1).
 *  - Single value
 *  - Iterator of values
 *  - Special iterator for gather operations
 *
 * @param <T1> Type output from the communication layer for the corresponding edge
 * @param <T0> Base type of the edge
 */
public interface TLink<T1, T0> extends TBase {

  /**
   * Name of the TSet and return the same tlink
   *
   * @param name name
   * @return this tlink
   */
  @Override
  TLink<T1, T0> setName(String name);

  /**
   * Creates a Compute {@link TSet} based on the {@link ComputeFunc} provided.
   *
   * @param computeFunction comp function. Takes in T0 type object and map to the output type O
   * @param <O>             output tset base type
   * @return Compute TSet
   */
  <O> TSet<O> compute(ComputeFunc<T1, O> computeFunction);

  /**
   * Creates a Compute {@link TSet} based on the {@link ComputeCollectorFunc} provided.
   *
   * @param computeFunction compute function with collector
   * @param <O>             output type (for the {@link edu.iu.dsc.tws.api.tset.fn.RecordCollector})
   * @return Compute TSet
   */
  <O> TSet<O> compute(ComputeCollectorFunc<T1, O> computeFunction);

  /**
   * Performs elementwise map operation based on the {@link MapFunc} provided
   *
   * @param mapFn map function T0 to O
   * @param <O>   output type
   * @return Compute tset
   */
  <O> TSet<O> map(MapFunc<T0, O> mapFn);

  /**
   * Performs flat map operation based on the {@link FlatMapFunc} provided
   *
   * @param mapFn map function which can produce multiple elements for a single {@literal <T0>} element
   * @param <O>   map function to T0 to multiple elements of {@literal <O>}
   * @return Compute TSet
   */
  <O> TSet<O> flatmap(FlatMapFunc<T0, O> mapFn);

  /**
   * Maps the data passed through the TLink to {@link Tuple} based on a {@link MapFunc} and thereby
   * creating a {@link TupleTSet}
   *
   * @param genTupleFn {@link MapFunc} to generate a {@link Tuple}
   * @param <K>        key type
   * @param <V>        value type
   * @return Keyed TSet
   */
  <K, V> TupleTSet<K, V> mapToTuple(MapFunc<T0, Tuple<K, V>> genTupleFn);

  /**
   * Computes the data passed through the TLink to {@link Tuple} based on a {@link MapFunc} and thereby
   * creating a {@link TupleTSet}
   *
   * @param computeFunc {@link MapFunc} to generate a {@link Tuple}
   * @param <K>        key type
   * @param <V>        value type
   * @return Keyed TSet
   */
  <K, V> TupleTSet<K, V> computeToTuple(ComputeFunc<T1, Tuple<K, V>> computeFunc);

  <K, V> TupleTSet<K, V> computeToTuple(ComputeCollectorFunc<T1, Tuple<K, V>> computeFunc);

  /**
   * Applies a function elementwise. Similar to compute, but the {@link ApplyFunc} does not
   * return anything.
   *
   * @param applyFunction apply function
   */
  void forEach(ApplyFunc<T0> applyFunction);

  /**
   * Creates a Sink TSet based on the {@link SinkFunc}.
   *
   * @param sinkFunction sink function which takes in {@literal <T1>}. Similar to a compute, but would not
   *                     return any TSet
   * @return Sink tset. This would would be a terminal TSet with no transformation capabilities.
   */
  TBase sink(SinkFunc<T1> sinkFunction);
}
