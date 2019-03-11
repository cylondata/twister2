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

import edu.iu.dsc.tws.api.tset.FlatMapFunction;
import edu.iu.dsc.tws.api.tset.IterableFlatMapFunction;
import edu.iu.dsc.tws.api.tset.IterableMapFunction;
import edu.iu.dsc.tws.api.tset.MapFunction;
import edu.iu.dsc.tws.api.tset.Sink;
import edu.iu.dsc.tws.api.tset.TBase;
import edu.iu.dsc.tws.api.tset.sets.FlatMapTSet;
import edu.iu.dsc.tws.api.tset.sets.IFlatMapTSet;
import edu.iu.dsc.tws.api.tset.sets.IMapTSet;
import edu.iu.dsc.tws.api.tset.sets.MapTSet;
import edu.iu.dsc.tws.api.tset.sets.SinkTSet;

/**
 * Link represents the connections between data Links.
 * This would map to some form of communication patter in the communication layer
 */
public interface TLink<T> extends TBase<T> {

  /**
   * Name of the tset
   */
  TLink<T> setName(String name);

  /**
   * Link the parallelism for this link
   *
   * @param parallelism parallelism
   * @return this link
   */
  TLink<T> setParallelism(int parallelism);

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
   * Add a sink
   *
   * @param sink sink function
   */
  SinkTSet<T> sink(Sink<T> sink);
}
