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
import edu.iu.dsc.tws.api.tset.TSetEnvironment;
import edu.iu.dsc.tws.api.tset.fn.MapFunction;
import edu.iu.dsc.tws.api.tset.fn.PartitionFunction;
import edu.iu.dsc.tws.api.tset.fn.ReduceFunction;
import edu.iu.dsc.tws.api.tset.link.ReduceTLink;
import edu.iu.dsc.tws.api.tset.link.streaming.StreamingAllGatherTLink;
import edu.iu.dsc.tws.api.tset.link.streaming.StreamingAllReduceTLink;
import edu.iu.dsc.tws.api.tset.link.streaming.StreamingDirectTLink;
import edu.iu.dsc.tws.api.tset.link.streaming.StreamingGatherTLink;
import edu.iu.dsc.tws.api.tset.link.streaming.StreamingPartitionTLink;
import edu.iu.dsc.tws.api.tset.link.streaming.StreamingReplicateTLink;
import edu.iu.dsc.tws.api.tset.sets.BaseTSet;
import edu.iu.dsc.tws.api.tset.sets.CachedTSet;
import edu.iu.dsc.tws.api.tset.sets.KeyedTSet;

public abstract class StreamingBaseTSet<T> extends BaseTSet<T> {

  public StreamingBaseTSet(TSetEnvironment tSetEnv, String name, int parallelism) {
    super(tSetEnv, name, parallelism);
  }

  @Override
  public StreamingDirectTLink<T> direct() {
    StreamingDirectTLink<T> direct = new StreamingDirectTLink<>(getTSetEnv(), getParallelism());
    addChildToGraph(direct);
    return direct;
  }

  @Override
  public ReduceTLink<T> reduce(ReduceFunction<T> reduceFn) {
    return null;
  }

  public StreamingPartitionTLink<T> partition(PartitionFunction<T> partitionFn) {
    StreamingPartitionTLink<T> partition = new StreamingPartitionTLink<>(getTSetEnv(),
        partitionFn, getParallelism());
    addChildToGraph(partition);
    return partition;
  }

  @Override
  public StreamingGatherTLink<T> gather() {
    StreamingGatherTLink<T> gather = new StreamingGatherTLink<>(getTSetEnv(), getParallelism());
    addChildToGraph(gather);
    return gather;
  }

  @Override
  public StreamingAllReduceTLink<T> allReduce(ReduceFunction<T> reduceFn) {
    StreamingAllReduceTLink<T> allreduce = new StreamingAllReduceTLink<>(getTSetEnv(), reduceFn,
        getParallelism());
    addChildToGraph(allreduce);
    return allreduce;
  }

  @Override
  public StreamingAllGatherTLink<T> allGather() {
    StreamingAllGatherTLink<T> allgather = new StreamingAllGatherTLink<>(getTSetEnv(),
        getParallelism());
    addChildToGraph(allgather);
    return allgather;
  }

  @Override
  public <K, V> KeyedTSet<K, V, T> mapToTuple(MapFunction<Tuple<K, V>, T> mapToTupleFn) {
    throw new UnsupportedOperationException("Groupby is not avilable in streaming operations");
  }

  @Override
  public StreamingReplicateTLink<T> replicate(int replications) {
    if (getParallelism() != 1) {
      throw new RuntimeException("Only tsets with parallelism 1 can be replicated: "
          + getParallelism());
    }

    StreamingReplicateTLink<T> cloneTSet = new StreamingReplicateTLink<>(getTSetEnv(),
        replications);
    addChildToGraph(cloneTSet);
    return cloneTSet;
  }

  @Override
  public CachedTSet<T> cache() {
    throw new UnsupportedOperationException("Cache is not avilable in streaming operations");
  }

/*  public void sink(Sink<T> sink) {
    StreamingDirectTLink<T> direct = new StreamingDirectTLink<>(getTSetEnv(), getParallelism());
    addChildToGraph(direct);
    direct.sink(sink);
  }*/
}
