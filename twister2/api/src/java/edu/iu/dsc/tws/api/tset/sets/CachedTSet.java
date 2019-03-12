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

package edu.iu.dsc.tws.api.tset.sets;

import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.api.tset.Cacheable;
import edu.iu.dsc.tws.api.tset.FlatMapFunction;
import edu.iu.dsc.tws.api.tset.IterableFlatMapFunction;
import edu.iu.dsc.tws.api.tset.IterableMapFunction;
import edu.iu.dsc.tws.api.tset.MapFunction;
import edu.iu.dsc.tws.api.tset.Sink;
import edu.iu.dsc.tws.api.tset.Source;
import edu.iu.dsc.tws.api.tset.TSet;
import edu.iu.dsc.tws.api.tset.TSetEnv;
import edu.iu.dsc.tws.api.tset.TSetUtils;
import edu.iu.dsc.tws.api.tset.link.BaseTLink;
import edu.iu.dsc.tws.api.tset.ops.SinkOp;
import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.dataset.DataObject;
import edu.iu.dsc.tws.dataset.DataObjectImpl;
import edu.iu.dsc.tws.dataset.impl.EntityPartition;

public class CachedTSet<T> extends BaseTSet<T> implements Cacheable<T> {

  private BaseTLink<T> parent;
  // todo: This dataobject should bind to the executor, I think! because tsets would not be
  //  visible to the executor
  private DataObject<T> datapoints = null;

  public CachedTSet(Config cfg, TSetEnv tSetEnv, BaseTLink<T> prnt) {
    super(cfg, tSetEnv);
    this.parent = prnt;
    this.name = "cache-" + parent.getName();
    datapoints = new DataObjectImpl<>(config);

  }

  // todo: operations like map is different on a cached tset, because map will be done on data in
  //  the execution runtime, rather than a source task

  @Override
  public boolean baseBuild() {
    boolean isIterable = TSetUtils.isIterableInput(parent, tSetEnv.getTSetBuilder().getOpMode());
    boolean keyed = TSetUtils.isKeyedInput(parent);
    // lets override the parallelism
    int p = calculateParallelism(parent);
    Sink<T> cacheSink = new CacheSink();
    cacheSink.addInputs(inputMap);
    ComputeConnection connection = tSetEnv.getTSetBuilder().getTaskGraphBuilder().addSink(getName(),
        new SinkOp<>(cacheSink, isIterable, keyed), p);
    parent.buildConnection(connection);
    return true;
  }

  public <P> MapTSet<P, T> map(MapFunction<T, P> mapFn) {
    SourceTSet<T> cacheSource = (SourceTSet<T>) tSetEnv.createSource(new CacheSource());
    return cacheSource.map(mapFn);
  }

  public <P> FlatMapTSet<P, T> flatMap(FlatMapFunction<T, P> mapFn) {
    SourceTSet<T> cacheSource = (SourceTSet<T>) tSetEnv.createSource(new CacheSource());
    return cacheSource.flatMap(mapFn);
  }

  public <P1> IMapTSet<P1, T> map(IterableMapFunction<T, P1> mFn) {
    SourceTSet<T> cacheSource = (SourceTSet<T>) tSetEnv.createSource(new CacheSource());
    return cacheSource.map(mFn);
  }

  public <P1> IFlatMapTSet<P1, T> flatMap(IterableFlatMapFunction<T, P1> mFn) {
    SourceTSet<T> cacheSource = (SourceTSet<T>) tSetEnv.createSource(new CacheSource());
    return cacheSource.flatMap(mFn);
  }

  public SinkTSet<T> sink(Sink<T> sink) {
    SourceTSet<T> cacheSource = (SourceTSet<T>) tSetEnv.createSource(new CacheSource());
    return cacheSource.sink(sink);
  }

  @Override
  public void buildConnection(ComputeConnection connection) {
    throw new IllegalStateException("Build connections should not be called on a TSet");
  }

  @Override
  public DataObject<T> getData() {
    return datapoints;
  }

  @Override
  public boolean addData(T value) {
    int curr = datapoints.getPartitionCount();
    datapoints.addPartition(new EntityPartition<T>(curr, value)); //
    return false;
  }

  @Override
  public CachedTSet<T> setParallelism(int parallelism) {
    this.parallel = parallelism;
    return this;
  }

  @Override
  public CachedTSet<T> setName(String n) {
    this.name = n;
    return this;
  }

  private class CacheSink implements Sink<T> {

    private int count = 0;

    @Override
    public boolean add(T value) {
      // todo every time add is called, a new partition will be made! how to handle that?
      return addData(value);
    }

    @Override
    public void close() {

    }
  }

  private class CacheSource implements Source<T> {
    //TODO: need to check this codes logic developed now just based on the data object API
    private int count = getData().getPartitionCount();
    private int current = 0;

    @Override
    public boolean hasNext() {
      boolean hasNext = (current < count) ? getData().getPartitions(current)
          .getConsumer().hasNext() : false;

      while (++current < count && !hasNext) {
        hasNext = getData().getPartitions(current).getConsumer().hasNext();
      }
      return hasNext;
    }

    @Override
    public T next() {
      return getData().getPartitions(current).getConsumer().next();
    }
  }
}
