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


package edu.iu.dsc.tws.tset.links.batch;

import java.util.Iterator;

import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.dataset.DataObject;
import edu.iu.dsc.tws.api.tset.fn.ApplyFunc;
import edu.iu.dsc.tws.api.tset.fn.FlatMapFunc;
import edu.iu.dsc.tws.api.tset.fn.MapFunc;
import edu.iu.dsc.tws.api.tset.link.batch.BatchTupleMappableLink;
import edu.iu.dsc.tws.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.tset.fn.FlatMapIterCompute;
import edu.iu.dsc.tws.tset.fn.ForEachIterCompute;
import edu.iu.dsc.tws.tset.fn.MapIterCompute;
import edu.iu.dsc.tws.tset.ops.MapToTupleIterOp;
import edu.iu.dsc.tws.tset.sets.batch.CachedTSet;
import edu.iu.dsc.tws.tset.sets.batch.ComputeTSet;
import edu.iu.dsc.tws.tset.sets.batch.KeyedTSet;
import edu.iu.dsc.tws.tset.sinks.CacheIterSink;

public abstract class BIteratorLink<T> extends BBaseTLink<Iterator<T>, T>
    implements BatchTupleMappableLink<T> {

  private CachedTSet<T> savedCacheTSet;

  BIteratorLink(BatchTSetEnvironment env, String n, int sourceP) {
    this(env, n, sourceP, sourceP);
  }

  BIteratorLink(BatchTSetEnvironment env, String n, int sourceP, int targetP) {
    super(env, n, sourceP, targetP);
  }

  @Override
  public <P> ComputeTSet<P, Iterator<T>> map(MapFunc<P, T> mapFn) {
    return compute("map", new MapIterCompute<>(mapFn));
  }

  @Override
  public <P> ComputeTSet<P, Iterator<T>> flatmap(FlatMapFunc<P, T> mapFn) {
    return compute("flatmap", new FlatMapIterCompute<>(mapFn));
  }

  @Override
  public void forEach(ApplyFunc<T> applyFunction) {
    ComputeTSet<Object, Iterator<T>> set = compute("foreach",
        new ForEachIterCompute<>(applyFunction)
    );

    getTSetEnv().run(set);
  }

  @Override
  public <K, V> KeyedTSet<K, V> mapToTuple(MapFunc<Tuple<K, V>, T> mapToTupFn) {
    KeyedTSet<K, V> set = new KeyedTSet<>(getTSetEnv(), new MapToTupleIterOp<>(mapToTupFn),
        getTargetParallelism());

    addChildToGraph(set);

    return set;
  }

  @Override
  public CachedTSet<T> cache(boolean isIterative) {
    CachedTSet<T> cacheTSet;
    if (isIterative && savedCacheTSet != null) {
      cacheTSet = savedCacheTSet;
    } else {
      cacheTSet = new CachedTSet<>(getTSetEnv(), new CacheIterSink<T>(),
          getTargetParallelism());
      savedCacheTSet = cacheTSet;
      addChildToGraph(cacheTSet);
    }

    DataObject<T> output = getTSetEnv().runAndGet(cacheTSet, isIterative);
    cacheTSet.setData(output);

    return cacheTSet;
  }

  @Override
  public CachedTSet<T> cache() {
    CachedTSet<T> cacheTSet = new CachedTSet<>(getTSetEnv(), new CacheIterSink<T>(),
        getTargetParallelism());
    addChildToGraph(cacheTSet);

    DataObject<T> output = getTSetEnv().runAndGet(cacheTSet);
    cacheTSet.setData(output);
    return cacheTSet;
  }

  public void finishIter() {
    getTSetEnv().finishIter();
  }
}
