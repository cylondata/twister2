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

import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.dataset.DataObject;
import edu.iu.dsc.tws.api.task.nodes.ICompute;
import edu.iu.dsc.tws.api.tset.Cacheable;
import edu.iu.dsc.tws.api.tset.TSetEnvironment;
import edu.iu.dsc.tws.api.tset.TSetUtils;
import edu.iu.dsc.tws.api.tset.fn.MapFunc;
import edu.iu.dsc.tws.api.tset.fn.PartitionFunc;
import edu.iu.dsc.tws.api.tset.fn.ReduceFunc;
import edu.iu.dsc.tws.api.tset.fn.SinkFunc;
import edu.iu.dsc.tws.api.tset.link.AllGatherTLink;
import edu.iu.dsc.tws.api.tset.link.AllReduceTLink;
import edu.iu.dsc.tws.api.tset.link.DirectTLink;
import edu.iu.dsc.tws.api.tset.link.GatherTLink;
import edu.iu.dsc.tws.api.tset.link.PartitionTLink;
import edu.iu.dsc.tws.api.tset.link.ReduceTLink;
import edu.iu.dsc.tws.api.tset.link.ReplicateTLink;
import edu.iu.dsc.tws.api.tset.ops.SinkOp;
import edu.iu.dsc.tws.api.tset.sources.CacheSource;
import edu.iu.dsc.tws.dataset.DataObjectImpl;

/**
 * Cached tset
 *
 * @param <T> base type of the tset
 */
public class CachedTSet<T> extends BatchBaseTSet<T> implements Cacheable<T> {
  // todo: This dataobject should bind to the executor, I think! because tsets would not be
  //  visible to the executor
  private DataObject<T> data;

  private SinkOp<?> sinkOp;

  public CachedTSet(TSetEnvironment tSetEnv, SinkFunc<?> sinkFunc, int parallelism) {
    super(tSetEnv, TSetUtils.generateName("cached"), parallelism);
    this.data = new DataObjectImpl<>(tSetEnv.getConfig());

    this.sinkOp = new SinkOp<>(sinkFunc);
  }

  @Override
  public ICompute getINode() {
    return sinkOp;
  }

  @Override
  public DirectTLink<T> direct() {
    BatchSourceTSet<T> cacheSource = getTSetEnv().createBatchSource(new CacheSource<>(data),
        getParallelism()).setName(TSetUtils.generateName(generatePrefix()));
    return cacheSource.direct();
  }

  @Override
  public ReduceTLink<T> reduce(ReduceFunc<T> reduceFn) {
    BatchSourceTSet<T> cacheSource = getTSetEnv().createBatchSource(new CacheSource<>(data),
        getParallelism()).setName(TSetUtils.generateName(generatePrefix()));
    return cacheSource.reduce(reduceFn);
  }

  @Override
  public PartitionTLink<T> partition(PartitionFunc<T> partitionFn) {
    BatchSourceTSet<T> cacheSource = getTSetEnv().createBatchSource(new CacheSource<>(data),
        getParallelism()).setName(TSetUtils.generateName(generatePrefix()));
    return cacheSource.partition(partitionFn);
  }

  @Override
  public GatherTLink<T> gather() {
    BatchSourceTSet<T> cacheSource = getTSetEnv().createBatchSource(new CacheSource<>(data),
        getParallelism()).setName(TSetUtils.generateName(generatePrefix()));
    return cacheSource.gather();
  }

  @Override
  public AllReduceTLink<T> allReduce(ReduceFunc<T> reduceFn) {
    BatchSourceTSet<T> cacheSource = getTSetEnv().createBatchSource(new CacheSource<>(data),
        getParallelism()).setName(TSetUtils.generateName(generatePrefix()));
    return cacheSource.allReduce(reduceFn);
  }

  @Override
  public AllGatherTLink<T> allGather() {
    BatchSourceTSet<T> cacheSource = getTSetEnv().createBatchSource(new CacheSource<>(data),
        getParallelism()).setName(TSetUtils.generateName(generatePrefix()));
    return cacheSource.allGather();
  }

  @Override
  public <K, V> KeyedTSet<K, V, T> mapToTuple(MapFunc<Tuple<K, V>, T> generateTuple) {
    BatchSourceTSet<T> cacheSource = getTSetEnv().createBatchSource(new CacheSource<>(data),
        getParallelism()).setName(TSetUtils.generateName(generatePrefix()));
    return cacheSource.mapToTuple(generateTuple);
  }

  @Override
  public ReplicateTLink<T> replicate(int replications) {
    BatchSourceTSet<T> cacheSource = getTSetEnv().createBatchSource(new CacheSource<>(data),
        getParallelism()).setName(TSetUtils.generateName(generatePrefix()));
    return cacheSource.replicate(replications);
  }

  @Override
  public CachedTSet<T> cache() {
    return this;
//    throw new IllegalStateException("Calling Cache on an already cached Object");
  }

  @Override
  public DataObject<T> getDataObject() {
    return data;
  }

  private String generatePrefix() {
    return "csource(" + data.getID() + ")";
  }

/*  @Override
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
  }*/

  public void setData(DataObject<T> inData) {
    this.data = inData;
  }

  @Override
  public CachedTSet<T> setName(String n) {
    rename(n);
    return this;
  }
}
