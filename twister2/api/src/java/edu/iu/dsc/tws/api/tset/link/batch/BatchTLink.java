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

package edu.iu.dsc.tws.api.tset.link.batch;

import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.tset.StoringData;
import edu.iu.dsc.tws.api.tset.TBase;
import edu.iu.dsc.tws.api.tset.fn.ApplyFunc;
import edu.iu.dsc.tws.api.tset.fn.ComputeCollectorFunc;
import edu.iu.dsc.tws.api.tset.fn.ComputeFunc;
import edu.iu.dsc.tws.api.tset.fn.FlatMapFunc;
import edu.iu.dsc.tws.api.tset.fn.MapFunc;
import edu.iu.dsc.tws.api.tset.fn.SinkFunc;
import edu.iu.dsc.tws.api.tset.link.TLink;
import edu.iu.dsc.tws.api.tset.sets.batch.BatchTSet;
import edu.iu.dsc.tws.api.tset.sets.batch.BatchTupleTSet;

/**
 * Batch extension of {@link TLink} with the capability of {@link StoringData}. Also overrides
 * the return types to match batch operations.
 *
 * @param <T1> comms data type
 * @param <T0> base data type
 */
public interface BatchTLink<T1, T0> extends TLink<T1, T0>, StoringData<T0> {

  @Override
  BatchTLink<T1, T0> setName(String name);

  @Override
  <O> BatchTSet<O> compute(ComputeFunc<O, T1> computeFunction);

  @Override
  <O> BatchTSet<O> compute(ComputeCollectorFunc<O, T1> computeFunction);

  @Override
  <O> BatchTSet<O> map(MapFunc<O, T0> mapFn);

  @Override
  <O> BatchTSet<O> flatmap(FlatMapFunc<O, T0> mapFn);

  @Override
  <K, V> BatchTupleTSet<K, V> mapToTuple(MapFunc<Tuple<K, V>, T0> genTupleFn);

  @Override
  TBase sink(SinkFunc<T1> sinkFunction);

  @Override
  void forEach(ApplyFunc<T0> applyFunction);

  BatchTSet<Object> lazyForEach(ApplyFunc<T0> applyFunction);
}
