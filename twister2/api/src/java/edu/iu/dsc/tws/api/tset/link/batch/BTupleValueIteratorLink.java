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

import java.util.Iterator;

import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.dataset.DataObject;
import edu.iu.dsc.tws.api.tset.TSetUtils;
import edu.iu.dsc.tws.api.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.api.tset.fn.ApplyFunc;
import edu.iu.dsc.tws.api.tset.fn.FlatMapFunc;
import edu.iu.dsc.tws.api.tset.fn.FlatMapTupleValueIterCompute;
import edu.iu.dsc.tws.api.tset.fn.ForEachTupleValueIterCompute;
import edu.iu.dsc.tws.api.tset.fn.MapFunc;
import edu.iu.dsc.tws.api.tset.fn.MapTupleValueIterCompute;
import edu.iu.dsc.tws.api.tset.sets.batch.CachedTSet;
import edu.iu.dsc.tws.api.tset.sets.batch.ComputeTSet;
import edu.iu.dsc.tws.api.tset.sinks.CacheTupleValueIterSink;

/**
 * This is the Tlinks used by gather operations. Specific operations such as map, flatmap, cache,
 * etc will be done on the tuple value only (key will be dropped, as key is an information
 * forcibly attached at the communication level). If the key information is required, users can
 * use the compute methods which enables the use of Iterator<Tuple<K, T>>
 * @param <K> key type
 * @param <T> value type
 */
public abstract class BTupleValueIteratorLink<K, T> extends
    BBaseTLink<Iterator<Tuple<K, T>>, T> {

  BTupleValueIteratorLink(BatchTSetEnvironment env, String n, int sourceP) {
    this(env, n, sourceP, sourceP);
  }

  BTupleValueIteratorLink(BatchTSetEnvironment env, String n, int sourceP, int targetP) {
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
    CachedTSet<T> cacheTSet = new CachedTSet<>(getTSetEnv(), new CacheTupleValueIterSink<K, T>(),
        getTargetParallelism());
    addChildToGraph(cacheTSet);

    DataObject<T> output = getTSetEnv().runAndGet(cacheTSet);
    cacheTSet.setData(output);

    return cacheTSet;
  }
}
