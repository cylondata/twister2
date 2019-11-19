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
import edu.iu.dsc.tws.api.tset.Storable;
import edu.iu.dsc.tws.api.tset.fn.ApplyFunc;
import edu.iu.dsc.tws.api.tset.fn.FlatMapFunc;
import edu.iu.dsc.tws.api.tset.fn.MapFunc;
import edu.iu.dsc.tws.api.tset.link.batch.BatchTupleMappableLink;
import edu.iu.dsc.tws.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.tset.fn.FlatMapIterCompute;
import edu.iu.dsc.tws.tset.fn.ForEachIterCompute;
import edu.iu.dsc.tws.tset.fn.MapIterCompute;
import edu.iu.dsc.tws.tset.sets.batch.CachedTSet;
import edu.iu.dsc.tws.tset.sets.batch.ComputeTSet;
import edu.iu.dsc.tws.tset.sets.batch.KeyedTSet;
import edu.iu.dsc.tws.tset.sets.batch.PersistedTSet;
import edu.iu.dsc.tws.tset.sinks.CacheIterSink;
import edu.iu.dsc.tws.tset.sinks.DiskPersistIterSink;

public abstract class BatchIteratorLink<T> extends BatchTLinkImpl<Iterator<T>, T>
    implements BatchTupleMappableLink<T> {

  BatchIteratorLink(BatchTSetEnvironment env, String n, int sourceP) {
    this(env, n, sourceP, sourceP);
  }

  BatchIteratorLink(BatchTSetEnvironment env, String n, int sourceP, int targetP) {
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
  public <K, V> KeyedTSet<K, V> mapToTuple(MapFunc<Tuple<K, V>, T> mapToTupFn) {
    KeyedTSet<K, V> set = new KeyedTSet<>(getTSetEnv(), new MapIterCompute<>(mapToTupFn),
        getTargetParallelism());

    addChildToGraph(set);

    return set;
  }

  @Override
  public ComputeTSet<Object, Iterator<T>> lazyForEach(ApplyFunc<T> applyFunction) {
    return compute("foreach", new ForEachIterCompute<>(applyFunction));
  }

  @Override
  public void forEach(ApplyFunc<T> applyFunction) {
    ComputeTSet<Object, Iterator<T>> set = lazyForEach(applyFunction);

    getTSetEnv().run(set);
  }

  /*
   * Returns the superclass @Storable<T> because, this class is used by both keyed and non-keyed
   * TSets. Hence, it produces both CachedTSet<T> as well as KeyedCachedTSet<K, V>
   */
  @Override
  public Storable<T> lazyCache() {
    CachedTSet<T> cacheTSet = new CachedTSet<>(getTSetEnv(), new CacheIterSink<T>(),
        getTargetParallelism());
    addChildToGraph(cacheTSet);

    return cacheTSet;
  }

  @Override
  public Storable<T> lazyPersist() {
    PersistedTSet<T> persistedTSet = new PersistedTSet<>(getTSetEnv(),
        new DiskPersistIterSink<>(this.getId()),
        getTargetParallelism());
    addChildToGraph(persistedTSet);

    return persistedTSet;
  }

  @Override
  public Storable<T> cache() {
    return super.cache();
  }

  @Override
  public Storable<T> persist() {
    return super.persist();
  }
}
