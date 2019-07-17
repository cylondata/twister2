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

import java.util.ArrayList;
import java.util.List;

import edu.iu.dsc.tws.api.dataset.DataObject;
import edu.iu.dsc.tws.api.dataset.DataPartition;
import edu.iu.dsc.tws.api.task.nodes.ICompute;
import edu.iu.dsc.tws.api.tset.Cacheable;
import edu.iu.dsc.tws.api.tset.TSetEnvironment;
import edu.iu.dsc.tws.api.tset.TSetGraph;
import edu.iu.dsc.tws.api.tset.TSetUtils;
import edu.iu.dsc.tws.api.tset.fn.PartitionFunction;
import edu.iu.dsc.tws.api.tset.fn.ReduceFunction;
import edu.iu.dsc.tws.api.tset.fn.Selector;
import edu.iu.dsc.tws.api.tset.link.AllGatherTLink;
import edu.iu.dsc.tws.api.tset.link.AllReduceTLink;
import edu.iu.dsc.tws.api.tset.link.DirectTLink;
import edu.iu.dsc.tws.api.tset.link.GatherTLink;
import edu.iu.dsc.tws.api.tset.link.PartitionTLink;
import edu.iu.dsc.tws.api.tset.link.ReduceTLink;
import edu.iu.dsc.tws.api.tset.link.ReplicateTLink;
import edu.iu.dsc.tws.api.tset.sources.CacheSource;
import edu.iu.dsc.tws.dataset.DataObjectImpl;

public class CachedTSet<T> extends BatchBaseTSet<T> implements Cacheable<T> {
  // todo: This dataobject should bind to the executor, I think! because tsets would not be
  //  visible to the executor
  private DataObject<T> data;

  public CachedTSet(TSetEnvironment tSetEnv, int parallelism) {
    super(tSetEnv, TSetUtils.generateName("cached"), parallelism);
    data = new DataObjectImpl<>(tSetEnv.getConfig());
  }

  // todo: operations like map is different on a cached tset, because map will be done on data in
  //  the execution runtime, rather than a source task

  @Override
  public void build(TSetGraph tSetGraph) {
//    boolean isIterable = TSetUtils.isIterableInput(parent, tSetEnv.getTSetBuilder().getOpMode());
//    boolean keyed = TSetUtils.isKeyedInput(parent);
//    // lets override the parallelism
//    int para = calculateParallelism(parent);
//    Sink<T> cacheSink = new CacheSink();
//    ComputeConnection connection =
//    tSetEnv.getTSetBuilder().getTaskGraphBuilder().addSink(getName(),
//        new SinkOp<>(cacheSink, isIterable, keyed), para);
//    parent.buildConnection(connection);
//    return true;
  }

  @Override
  protected ICompute getTask() {
    return null;
  }

//  public <P1> IterableMapTSet<T, P1> map(IterableMapFunction<T, P1> mFn) {
//    BatchSourceTSet<T> cacheSource = getTSetEnv().createBatchSource(new CacheSource<>(data),
//        getParallelism());
//    return cacheSource.map(mFn);
//  }
//
//  public <P1> IterableFlatMapTSet<T, P1> flatMap(IterableFlatMapFunction<T, P1> mFn) {
//    BatchSourceTSet<T> cacheSource = getTSetEnv().createBatchSource(new CacheSource<>(data),
//        getParallelism());
//    return cacheSource.flatMap(mFn);
//  }

/*  public SinkTSet<T> sink(Sink<T> sink) {
    BatchSourceTSet<T> cacheSource = getTSetEnv().createBatchSource(new CacheSource<>(data),
        getParallelism());
    return cacheSource.sink(sink);
  }*/

  @Override
  public DirectTLink<T> direct() {
    BatchSourceTSet<T> cacheSource = getTSetEnv().createBatchSource(new CacheSource<>(data),
        getParallelism());
    return cacheSource.direct();
  }

  @Override
  public ReduceTLink<T> reduce(ReduceFunction<T> reduceFn) {
    BatchSourceTSet<T> cacheSource = getTSetEnv().createBatchSource(new CacheSource<>(data),
        getParallelism());
    return cacheSource.reduce(reduceFn);
  }

  @Override
  public PartitionTLink<T> partition(PartitionFunction<T> partitionFn) {
    BatchSourceTSet<T> cacheSource = getTSetEnv().createBatchSource(new CacheSource<>(data),
        getParallelism());
    return cacheSource.partition(partitionFn);
  }

  @Override
  public GatherTLink<T> gather() {
    BatchSourceTSet<T> cacheSource = getTSetEnv().createBatchSource(new CacheSource<>(data),
        getParallelism());
    return cacheSource.gather();
  }

  @Override
  public AllReduceTLink<T> allReduce(ReduceFunction<T> reduceFn) {
    BatchSourceTSet<T> cacheSource = getTSetEnv().createBatchSource(new CacheSource<>(data),
        getParallelism());
    return cacheSource.allReduce(reduceFn);
  }

  @Override
  public AllGatherTLink<T> allGather() {
    BatchSourceTSet<T> cacheSource = getTSetEnv().createBatchSource(new CacheSource<>(data),
        getParallelism());
    return cacheSource.allGather();
  }

  @Override
  public <K> GroupedTSet<K, T> groupBy(PartitionFunction<K> partitionFunction,
                                       Selector<K, T> selector) {
    BatchSourceTSet<T> cacheSource = getTSetEnv().createBatchSource(new CacheSource<>(data),
        getParallelism());
    return cacheSource.groupBy(partitionFunction, selector);
  }

  @Override
  public ReplicateTLink<T> replicate(int replications) {
    BatchSourceTSet<T> cacheSource = getTSetEnv().createBatchSource(new CacheSource<>(data),
        getParallelism());
    return cacheSource.replicate(replications);
  }

  @Override
  public CachedTSet<T> cache() {
    throw new IllegalStateException("Calling Cache on an already cached Object");
  }


  @Override
  public List<T> getData() {
    if (data == null) {
      return new ArrayList<>();
    }

    DataPartition<T>[] parts = data.getPartitions();
    List<T> results = new ArrayList<>();
    for (DataPartition<T> part : parts) {
      results.add(part.getConsumer().next());
    }
    return results;
  }

  @Override
  public DataObject<T> getDataObject() {
    return data;
  }

  @Override
  public T getPartitionData(int partitionId) {
    return null;
  }

  @Override
  public boolean addData(T value) {
//    if (data == null) {
//      data = new DataObjectImpl<>(config);
//    }
//    int curr = data.getPartitionCount();
//    data.addPartition(new EntityPartition<T>(curr, value)); //
    return false;
  }

  public void setData(DataObject<T> inData) {
    this.data = inData;
  }

  @Override
  public CachedTSet<T> setName(String n) {
    rename(n);
    return this;
  }
}
