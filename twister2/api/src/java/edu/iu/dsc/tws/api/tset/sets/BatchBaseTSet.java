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
import edu.iu.dsc.tws.api.tset.TSetEnvironment;
import edu.iu.dsc.tws.api.tset.fn.MapFunc;
import edu.iu.dsc.tws.api.tset.fn.PartitionFunc;
import edu.iu.dsc.tws.api.tset.fn.ReduceFunc;
import edu.iu.dsc.tws.api.tset.link.AllGatherTLink;
import edu.iu.dsc.tws.api.tset.link.AllReduceTLink;
import edu.iu.dsc.tws.api.tset.link.DirectTLink;
import edu.iu.dsc.tws.api.tset.link.GatherTLink;
import edu.iu.dsc.tws.api.tset.link.PartitionTLink;
import edu.iu.dsc.tws.api.tset.link.ReduceTLink;
import edu.iu.dsc.tws.api.tset.link.ReplicateTLink;

public abstract class BatchBaseTSet<T> extends BaseTSet<T> {

  BatchBaseTSet(TSetEnvironment tSetEnv, String name, int parallelism) {
    super(tSetEnv, name, parallelism);
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

  public PartitionTLink<T> partition(PartitionFunc<T> partitionFn) {
    PartitionTLink<T> partition = new PartitionTLink<>(getTSetEnv(), partitionFn, getParallelism());
    addChildToGraph(partition);
    return partition;
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
  public <K, V> KeyedTSet<K, V, T> mapToTuple(MapFunc<Tuple<K, V>, T> mapToTupleFn) {
    return direct().mapToTuple(mapToTupleFn);
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
  public CacheableTSet<T> cache() {
    return direct().cache();
  }
}
