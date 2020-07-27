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
import edu.iu.dsc.tws.api.compute.OperationNames;
import edu.iu.dsc.tws.api.compute.graph.Edge;
import edu.iu.dsc.tws.api.tset.fn.ApplyFunc;
import edu.iu.dsc.tws.api.tset.fn.FlatMapFunc;
import edu.iu.dsc.tws.api.tset.fn.MapFunc;
import edu.iu.dsc.tws.api.tset.schema.Schema;
import edu.iu.dsc.tws.tset.env.BatchEnvironment;
import edu.iu.dsc.tws.tset.fn.FlatMapCompute;
import edu.iu.dsc.tws.tset.fn.ForEachCompute;
import edu.iu.dsc.tws.tset.fn.MapCompute;
import edu.iu.dsc.tws.tset.links.TLinkUtils;
import edu.iu.dsc.tws.tset.sets.batch.CachedTSet;
import edu.iu.dsc.tws.tset.sets.batch.ComputeTSet;
import edu.iu.dsc.tws.tset.sets.batch.KeyedTSet;
import edu.iu.dsc.tws.tset.sets.batch.PersistedTSet;
import edu.iu.dsc.tws.tset.sinks.StreamingCacheSink;
import edu.iu.dsc.tws.tset.sinks.StreamingDiskPersistSink;

public class PipeTLink<T> extends BatchTLinkImpl<T, T> {
  private PipeTLink() {
    //non arg constructor for kryp
  }

  public PipeTLink(BatchEnvironment tSetEnv, int sourceParallelism, Schema schema) {
    super(tSetEnv, "pipe", sourceParallelism, sourceParallelism, schema);
  }

  public PipeTLink(BatchEnvironment tSetEnv, String name, int sourceParallelism,
                     Schema schema) {
    super(tSetEnv, name, sourceParallelism, sourceParallelism, schema);
  }

  @Override
  public PipeTLink<T> setName(String name) {
    rename(name);
    return this;
  }

  @Override
  public void forEach(ApplyFunc<T> applyFunction) {
    ComputeTSet<Object> set = lazyForEach(applyFunction);
    getTSetEnv().run(set);
  }

  @Override
  public ComputeTSet<Object> lazyForEach(ApplyFunc<T> applyFunction) {
    ForEachCompute<T> compute = new ForEachCompute<>(applyFunction);
    return compute("foreach", compute);
  }

  @Override
  public <K, V> KeyedTSet<K, V> mapToTuple(MapFunc<T, Tuple<K, V>> genTupleFn) {
    KeyedTSet<K, V> set = new KeyedTSet<>(getTSetEnv(), new MapCompute<>(genTupleFn),
        getTargetParallelism(), getSchema());

    addChildToGraph(set);

    return set;
  }

  @Override
  public <O> ComputeTSet<O> flatmap(FlatMapFunc<T, O> mapFn) {
    FlatMapCompute<T, O> comp = new FlatMapCompute<>(mapFn);
    return compute("flatmap", comp);
  }

  @Override
  public <O> ComputeTSet<O> map(MapFunc<T, O> mapFn) {
    MapCompute<T, O> comp = new MapCompute<>(mapFn);
    return compute("map", comp);
  }

  @Override
  public Edge getEdge() {
    Edge e = new Edge(getId(), OperationNames.PIPE, this.getSchema().getDataType());
    TLinkUtils.generateCommsSchema(getSchema(), e);
    return e;
  }

  @Override
  public CachedTSet<T> lazyCache() {
    CachedTSet<T> cacheTSet = new CachedTSet<>(getTSetEnv(), new StreamingCacheSink<>(),
        getTargetParallelism(), getSchema());
    addChildToGraph(cacheTSet);
    return cacheTSet;
  }

  @Override
  public PersistedTSet<T> lazyPersist() {
    PersistedTSet<T> persistedTSet = new PersistedTSet<>(getTSetEnv(),
        new StreamingDiskPersistSink<>(this.getId()), getTargetParallelism(), getSchema());
    addChildToGraph(persistedTSet);
    return persistedTSet;
  }

  @Override
  public CachedTSet<T> cache() {
    return (CachedTSet<T>) super.cache();
  }

  @Override
  public PersistedTSet<T> persist() {
    return (PersistedTSet<T>) super.persist();
  }
}
