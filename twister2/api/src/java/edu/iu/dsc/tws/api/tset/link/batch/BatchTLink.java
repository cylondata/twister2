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

import edu.iu.dsc.tws.api.tset.fn.ApplyFunc;
import edu.iu.dsc.tws.api.tset.fn.ComputeCollectorFunc;
import edu.iu.dsc.tws.api.tset.fn.ComputeFunc;
import edu.iu.dsc.tws.api.tset.fn.FlatMapFunc;
import edu.iu.dsc.tws.api.tset.fn.MapFunc;
import edu.iu.dsc.tws.api.tset.fn.SinkFunc;
import edu.iu.dsc.tws.api.tset.link.TLink;
import edu.iu.dsc.tws.api.tset.sets.batch.BatchTSet;

public interface BatchTLink<T1, T0> extends TLink<T1, T0> {
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

  BatchTSet<Object> lazyForEach(ApplyFunc<T0> applyFunction);

  BatchTSet<T1> lazySink(SinkFunc<T1> sinkFunction);

//  BatchTSet<T0> lazyCache();

  /**
   * Runs the dataflow graph and caches data in memory
   *
   * @return output TSet
   */
  default BatchTSet<T0> cache(boolean isIterative) {
    throw new UnsupportedOperationException("Operation not implemented");
  }

  /**
   * Runs the dataflow graph and caches data in memory
   *
   * @return output TSet
   */
  default BatchTSet<T0> cache() {
    return cache(false);
  }

  @Override
  void forEach(ApplyFunc<T0> applyFunction);

  @Override
  void sink(SinkFunc<T1> sinkFunction);
}
