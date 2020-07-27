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

import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.tset.fn.ApplyFunc;
import edu.iu.dsc.tws.api.tset.fn.ComputeCollectorFunc;
import edu.iu.dsc.tws.api.tset.fn.ComputeFunc;
import edu.iu.dsc.tws.api.tset.fn.FlatMapFunc;
import edu.iu.dsc.tws.api.tset.fn.MapFunc;
import edu.iu.dsc.tws.api.tset.schema.Schema;
import edu.iu.dsc.tws.api.tset.sets.StorableTBase;
import edu.iu.dsc.tws.tset.env.BatchEnvironment;
import edu.iu.dsc.tws.tset.fn.FlatMapCompute;
import edu.iu.dsc.tws.tset.fn.ForEachCompute;
import edu.iu.dsc.tws.tset.fn.MapCompute;
import edu.iu.dsc.tws.tset.sets.batch.CachedTSet;
import edu.iu.dsc.tws.tset.sets.batch.ComputeTSet;
import edu.iu.dsc.tws.tset.sets.batch.KeyedTSet;
import edu.iu.dsc.tws.tset.sets.batch.PersistedTSet;
import edu.iu.dsc.tws.tset.sinks.CacheSingleSink;
import edu.iu.dsc.tws.tset.sinks.DiskPersistSingleSink;

public abstract class BatchSingleLink<T> extends BatchTLinkImpl<T, T> {

  BatchSingleLink(BatchEnvironment env, String n, int sourceP, Schema schema) {
    super(env, n, sourceP, sourceP, schema);
  }

  BatchSingleLink(BatchEnvironment env, String n, int sourceP, int targetP,
                  Schema schema) {
    super(env, n, sourceP, targetP, schema);
  }

  @Override
  public <P> ComputeTSet<P, T> map(MapFunc<T, P> mapFn) {
    return compute("map", new MapCompute<>(mapFn));
  }

  @Override
  public <P> ComputeTSet<P, T> flatmap(FlatMapFunc<T, P> mapFn) {
    return compute("flatmap", new FlatMapCompute<>(mapFn));
  }

  @Override
  public void forEach(ApplyFunc<T> applyFunction) {
    ComputeTSet<Object, T> set = lazyForEach(applyFunction);

    getTSetEnv().run(set);
  }

  @Override
  public ComputeTSet<Object, T> lazyForEach(ApplyFunc<T> applyFunction) {
    return compute("foreach", new ForEachCompute<>(applyFunction));
  }

  @Override
  public <K, O> KeyedTSet<K, O> mapToTuple(MapFunc<T, Tuple<K, O>> genTupleFn) {
    KeyedTSet<K, O> set = new KeyedTSet<>(getTSetEnv(), new MapCompute<>(genTupleFn),
        getTargetParallelism(), getSchema());

    addChildToGraph(set);

    return set;
  }

  @Override
  public StorableTBase<T> lazyCache() {
    CachedTSet<T> cacheTSet = new CachedTSet<>(getTSetEnv(), new CacheSingleSink<T>(),
        getTargetParallelism(), getSchema());
    addChildToGraph(cacheTSet);
    return cacheTSet;
  }

  @Override
  public StorableTBase<T> cache() {
    return (CachedTSet<T>) super.cache();
  }

  @Override
  public StorableTBase<T> lazyPersist() {
    DiskPersistSingleSink<T> diskPersistSingleSink = new DiskPersistSingleSink<>(
        this.getId()
    );
    PersistedTSet<T> persistedTSet = new PersistedTSet<>(getTSetEnv(),
        diskPersistSingleSink, getTargetParallelism(), getSchema());
    addChildToGraph(persistedTSet);
    return persistedTSet;
  }

  @Override
  public StorableTBase<T> persist() {
    return (PersistedTSet<T>) super.persist();
  }
}
