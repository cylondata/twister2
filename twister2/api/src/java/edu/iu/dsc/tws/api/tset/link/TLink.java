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

import edu.iu.dsc.tws.api.tset.fn.Apply;
import edu.iu.dsc.tws.api.tset.fn.Compute;
import edu.iu.dsc.tws.api.tset.fn.ComputeCollector;
import edu.iu.dsc.tws.api.tset.fn.FlatMapFunction;
import edu.iu.dsc.tws.api.tset.fn.MapFunction;
import edu.iu.dsc.tws.api.tset.fn.Sink;
import edu.iu.dsc.tws.api.tset.sets.TSet;

/**
 * Link represents the connections between data Links.
 * This would map to some form of communication patter in the communication layer
 *
 * @param <T1> Type output from the communication layer for the corresponding edge
 * @param <T0> Base type of the edge
 */
public interface TLink<T1, T0> extends BuildableTLink {

  /**
   * Name of the TSet and return the same tlink
   *
   * @param name name
   * @return same TLink
   */
  TLink<T1, T0> setName(String name);

  /**
   * Base compute implementation
   *
   * @param computeFunction comp function. Takes in T0 type object and map to the output type O
   * @param <O> output tset base type
   * @return output TSet
   */
  <O> TSet<O> compute(Compute<O, T1> computeFunction);

  /**
   * Base compute implementation which would take in a Collector<O>
   *
   * @param computeFunction compute function with collector
   * @param <O> output type (Collector type)
   * @return output TSet
   */
  <O> TSet<O> compute(ComputeCollector<O, T1> computeFunction);

  /**
   * Elementwise map operation
   *
   * @param mapFn map function T0 to O
   * @param <O> output type
   * @return output TSet
   */
  <O> TSet<O> map(MapFunction<O, T0> mapFn);

  /**
   * Flatmap operation
   *
   * @param mapFn map function which can produce multiple elements for a single <T0> element
   * @param <O> map function to T0 to multiple elements of <O>
   * @return output TSet
   */
  <O> TSet<O> flatmap(FlatMapFunction<O, T0> mapFn);

  /**
   * Applies a functoin elementwise
   *
   * @param applyFunction apply function
   */
  void forEach(Apply<T0> applyFunction);

  /**
   * Sink function
   *
   * @param sinkFunction sink function which takes in <T1>. Similar to a compute, but would not
   * return any TSet
   */
  void sink(Sink<T1> sinkFunction);
}
