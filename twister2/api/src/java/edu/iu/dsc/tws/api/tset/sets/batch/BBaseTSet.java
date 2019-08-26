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
package edu.iu.dsc.tws.api.tset.sets.batch;

import java.util.Collection;
import java.util.Iterator;

import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.tset.Cacheable;
import edu.iu.dsc.tws.api.tset.TSetUtils;
import edu.iu.dsc.tws.api.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.api.tset.fn.MapFunc;
import edu.iu.dsc.tws.api.tset.fn.MapIterCompute;
import edu.iu.dsc.tws.api.tset.fn.PartitionFunc;
import edu.iu.dsc.tws.api.tset.fn.ReduceFunc;
import edu.iu.dsc.tws.api.tset.link.batch.AllGatherTLink;
import edu.iu.dsc.tws.api.tset.link.batch.AllReduceTLink;
import edu.iu.dsc.tws.api.tset.link.batch.DirectTLink;
import edu.iu.dsc.tws.api.tset.link.batch.GatherTLink;
import edu.iu.dsc.tws.api.tset.link.batch.PartitionTLink;
import edu.iu.dsc.tws.api.tset.link.batch.ReduceTLink;
import edu.iu.dsc.tws.api.tset.link.batch.ReplicateTLink;
import edu.iu.dsc.tws.api.tset.sets.BaseTSet;
import edu.iu.dsc.tws.api.tset.sets.TSet;

public abstract class BBaseTSet<T> extends BaseTSet<T> implements BatchTSet<T> {

  BBaseTSet(BatchTSetEnvironment tSetEnv, String name, int parallelism) {
    super(tSetEnv, name, parallelism);
  }

  @Override
  public BatchTSetEnvironment getTSetEnv() {
    return (BatchTSetEnvironment) super.getTSetEnv();
  }

  @Override
  public DirectTLink<T> direct() {
    DirectTLink<T> direct = new DirectTLink<>(getTSetEnv(), getParallelism());
    addChildToGraph(direct);
    return direct;
  }

  @Override
  public ReduceTLink<T> reduce(ReduceFunc<T> reduceFn) {
    ReduceTLink<T> reduce = new ReduceTLink<>(getTSetEnv(), reduceFn, getParallelism());
    addChildToGraph(reduce);
    return reduce;
  }

  @Override
  public PartitionTLink<T> partition(PartitionFunc<T> partitionFn, int targetParallelism) {
    PartitionTLink<T> partition = new PartitionTLink<>(getTSetEnv(), partitionFn, getParallelism(),
        targetParallelism);
    addChildToGraph(partition);
    return partition;
  }

  @Override
  public PartitionTLink<T> partition(PartitionFunc<T> partitionFn) {
    return partition(partitionFn, getParallelism());
  }

  @Override
  public GatherTLink<T> gather() {
    GatherTLink<T> gather = new GatherTLink<>(getTSetEnv(), getParallelism());
    addChildToGraph(gather);
    return gather;
  }

  @Override
  public AllReduceTLink<T> allReduce(ReduceFunc<T> reduceFn) {
    AllReduceTLink<T> reduce = new AllReduceTLink<>(getTSetEnv(), reduceFn, getParallelism());
    addChildToGraph(reduce);
    return reduce;
  }

  @Override
  public AllGatherTLink<T> allGather() {
    AllGatherTLink<T> gather = new AllGatherTLink<>(getTSetEnv(), getParallelism());
    addChildToGraph(gather);
    return gather;
  }

  // todo: remove this direct() --> would be more efficient. can handle at the context write level
  @Override
  public <K, V> KeyedTSet<K, V> mapToTuple(MapFunc<Tuple<K, V>, T> mapToTupleFn) {
    return direct().mapToTuple(mapToTupleFn);
  }

  @Override
  public ComputeTSet<T, Iterator<T>> union(TSet<T> other) {

    if (this.getParallelism() != ((BBaseTSet) other).getParallelism()) {
      throw new IllegalStateException("Parallelism of the TSets need to be the same in order to"
          + "perform a union operation");
    }

    ComputeTSet<T, Iterator<T>> unionTSet = direct().compute(TSetUtils.generateName("union"),
        new MapIterCompute<>((MapFunc<T, T>) input -> input));
    // now the following relationship is created
    // this -- directThis -- unionTSet

    DirectTLink<T> directOther = new DirectTLink<>(getTSetEnv(), getParallelism());
    addChildToGraph((BBaseTSet) other, directOther);
    addChildToGraph(directOther, unionTSet);
    // now the following relationship is created
    // this __ directThis __ unionTSet
    // other __ directOther _/

    return unionTSet;
  }

  @Override
  public ComputeTSet<T, Iterator<T>> union(Collection<TSet<T>> tSets) {

    ComputeTSet<T, Iterator<T>> unionTSet = direct().compute(TSetUtils.generateName("union"),
        new MapIterCompute<>((MapFunc<T, T>) input -> input));
    // now the following relationship is created
    // this -- directThis -- unionTSet

    for (TSet<T> tSet : tSets) {
      if (this.getParallelism() != ((BBaseTSet) tSet).getParallelism()) {
        throw new IllegalStateException("Parallelism of the TSets need to be the same in order to"
            + "perform a union operation");
      }
      DirectTLink<T> directOther = new DirectTLink<>(getTSetEnv(), getParallelism());
      addChildToGraph((BBaseTSet) tSet, directOther);
      addChildToGraph(directOther, unionTSet);
      // now the following relationship is created
      // this __ directThis __ unionTSet
      // other __ directOther _/
    }

    return unionTSet;
  }

  @Override
  public ReplicateTLink<T> replicate(int replications) {
    if (getParallelism() != 1) {
      throw new RuntimeException("Replication can not be done on tsets with parallelism != 1");
    }

    ReplicateTLink<T> cloneTSet = new ReplicateTLink<>(getTSetEnv(), replications);
    addChildToGraph(cloneTSet);
    return cloneTSet;
  }

  @Override
  public CachedTSet<T> cache() {
    return direct().cache();
  }

  @Override
  public boolean addInput(String key, Cacheable<?> input) {
    getTSetEnv().addInput(getId(), key, input);
    return true;
  }
}
