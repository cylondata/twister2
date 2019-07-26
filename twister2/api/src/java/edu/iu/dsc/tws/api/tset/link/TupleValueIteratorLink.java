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

package edu.iu.dsc.tws.api.tset.link;

import java.util.Iterator;

import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.tset.TSetEnvironment;
import edu.iu.dsc.tws.api.tset.TSetUtils;
import edu.iu.dsc.tws.api.tset.fn.ApplyFunc;
import edu.iu.dsc.tws.api.tset.fn.FlatMapFunc;
import edu.iu.dsc.tws.api.tset.fn.FlatMapTupleValueIterCompute;
import edu.iu.dsc.tws.api.tset.fn.ForEachTupleValueIterCompute;
import edu.iu.dsc.tws.api.tset.fn.MapFunc;
import edu.iu.dsc.tws.api.tset.fn.MapTupleValueIterCompute;
import edu.iu.dsc.tws.api.tset.sets.CachedTSet;
import edu.iu.dsc.tws.api.tset.sets.ComputeTSet;

public abstract class TupleValueIteratorLink<K, T> extends BaseTLink<Iterator<Tuple<K, T>>, T> {

  protected TupleValueIteratorLink(TSetEnvironment env, String n, int sourceP) {
    this(env, n, sourceP, sourceP);
  }

  protected TupleValueIteratorLink(TSetEnvironment env, String n, int sourceP, int targetP) {
    super(env, n, sourceP, targetP);
  }
/*  public <P> ComputeTSet<P, Iterator<T>>
computeWithoutKey(Compute<P, Iterator<T>> computeFunction) {
    computeFnWrapper = new ComputeCollectorWrapper<P, K, T>(computeFunction);
    return null;
  }

  public <P> ComputeTSet<P, Iterator<T>>
  computeWithoutKey(ComputeCollector<P, Iterator<T>> computeFunction) {
    ComputeCollectorWrapper<P, K, T> computeFnWrapper =
        new ComputeCollectorWrapper<>(computeFunction);
    return compute(TSetUtils.generateName("computec"));
  }*/

  @Override
  public <O> ComputeTSet<O, Iterator<Tuple<K, T>>> map(MapFunc<O, T> mapFn) {
    MapTupleValueIterCompute<O, K, T> comp = new MapTupleValueIterCompute<>(mapFn);
    return compute(TSetUtils.generateName("map"), comp);
  }

  @Override
  public <O> ComputeTSet<O, Iterator<Tuple<K, T>>> flatmap(FlatMapFunc<O, T> mapFn) {
    FlatMapTupleValueIterCompute<O, K, T> comp = new FlatMapTupleValueIterCompute<>(mapFn);
    return compute(TSetUtils.generateName("map"), comp);
  }

  @Override
  public void forEach(ApplyFunc<T> applyFunction) {
    ForEachTupleValueIterCompute<K, T> comp = new ForEachTupleValueIterCompute<>(applyFunction);
    ComputeTSet<Object, Iterator<Tuple<K, T>>> foreach =
        compute(TSetUtils.generateName("foreach"), comp);
    addChildToGraph(foreach);
    getTSetEnv().run(foreach);
  }

  @Override
  public CachedTSet<T> cache() {
    return null;
  }
}
