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
package edu.iu.dsc.tws.api.tset.sets.streaming;


import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.tset.Cacheable;
import edu.iu.dsc.tws.api.tset.TSetEnvironment;
import edu.iu.dsc.tws.api.tset.env.StreamingTSetEnvironment;
import edu.iu.dsc.tws.api.tset.fn.MapFunc;
import edu.iu.dsc.tws.api.tset.fn.PartitionFunc;
import edu.iu.dsc.tws.api.tset.fn.ReduceFunc;
import edu.iu.dsc.tws.api.tset.link.streaming.SAllGatherTLink;
import edu.iu.dsc.tws.api.tset.link.streaming.SAllReduceTLink;
import edu.iu.dsc.tws.api.tset.link.streaming.SDirectTLink;
import edu.iu.dsc.tws.api.tset.link.streaming.SGatherTLink;
import edu.iu.dsc.tws.api.tset.link.streaming.SPartitionTLink;
import edu.iu.dsc.tws.api.tset.link.streaming.SReduceTLink;
import edu.iu.dsc.tws.api.tset.link.streaming.SReplicateTLink;
import edu.iu.dsc.tws.api.tset.sets.BaseTSet;
import edu.iu.dsc.tws.api.tset.sets.batch.KeyedTSet;

public abstract class SBaseTSet<T> extends BaseTSet<T> implements StreamingTSet<T> {

  SBaseTSet(TSetEnvironment tSetEnv, String name, int parallelism) {
    super(tSetEnv, name, parallelism);
  }

  @Override
  public StreamingTSetEnvironment getTSetEnv() {
    return (StreamingTSetEnvironment) super.getTSetEnv();
  }

  @Override
  public SDirectTLink<T> direct() {
    SDirectTLink<T> direct = new SDirectTLink<>(getTSetEnv(), getParallelism());
    addChildToGraph(direct);
    return direct;
  }

  @Override
  public SReduceTLink<T> reduce(ReduceFunc<T> reduceFn) {
    return null;
  }

  @Override
  public SPartitionTLink<T> partition(PartitionFunc<T> partitionFn, int targetParallelism) {
    SPartitionTLink<T> partition = new SPartitionTLink<>(getTSetEnv(),
        partitionFn, getParallelism(), targetParallelism);
    addChildToGraph(partition);
    return partition;
  }

  @Override
  public SPartitionTLink<T> partition(PartitionFunc<T> partitionFn) {
    return partition(partitionFn, getParallelism());
  }

  @Override
  public SGatherTLink<T> gather() {
    SGatherTLink<T> gather = new SGatherTLink<>(getTSetEnv(), getParallelism());
    addChildToGraph(gather);
    return gather;
  }

  @Override
  public SAllReduceTLink<T> allReduce(ReduceFunc<T> reduceFn) {
    SAllReduceTLink<T> allreduce = new SAllReduceTLink<>(getTSetEnv(), reduceFn,
        getParallelism());
    addChildToGraph(allreduce);
    return allreduce;
  }

  @Override
  public SAllGatherTLink<T> allGather() {
    SAllGatherTLink<T> allgather = new SAllGatherTLink<>(getTSetEnv(),
        getParallelism());
    addChildToGraph(allgather);
    return allgather;
  }

  @Override
  public <K, V> KeyedTSet<K, V, T> mapToTuple(MapFunc<Tuple<K, V>, T> mapToTupleFn) {
    throw new UnsupportedOperationException("Groupby is not avilable in streaming operations");
  }

  @Override
  public SReplicateTLink<T> replicate(int replications) {
    if (getParallelism() != 1) {
      throw new RuntimeException("Only tsets with parallelism 1 can be replicated: "
          + getParallelism());
    }

    SReplicateTLink<T> cloneTSet = new SReplicateTLink<>(getTSetEnv(),
        replications);
    addChildToGraph(cloneTSet);
    return cloneTSet;
  }

  @Override
  public boolean addInput(String key, Cacheable<?> input) {
    getTSetEnv().addInput(getName(), key, input);
    return true;
  }
}
