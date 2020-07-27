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
package edu.iu.dsc.tws.tset.sets.streaming;


import java.util.Collection;

import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.tset.fn.MapFunc;
import edu.iu.dsc.tws.api.tset.fn.PartitionFunc;
import edu.iu.dsc.tws.api.tset.fn.ReduceFunc;
import edu.iu.dsc.tws.api.tset.schema.Schema;
import edu.iu.dsc.tws.api.tset.sets.TSet;
import edu.iu.dsc.tws.api.tset.sets.streaming.StreamingTSet;
import edu.iu.dsc.tws.tset.env.StreamingEnvironment;
import edu.iu.dsc.tws.tset.fn.MapCompute;
import edu.iu.dsc.tws.tset.links.streaming.SAllGatherTLink;
import edu.iu.dsc.tws.tset.links.streaming.SAllReduceTLink;
import edu.iu.dsc.tws.tset.links.streaming.SDirectTLink;
import edu.iu.dsc.tws.tset.links.streaming.SGatherTLink;
import edu.iu.dsc.tws.tset.links.streaming.SPartitionTLink;
import edu.iu.dsc.tws.tset.links.streaming.SReduceTLink;
import edu.iu.dsc.tws.tset.links.streaming.SReplicateTLink;
import edu.iu.dsc.tws.tset.sets.BaseTSetWithSchema;

public abstract class StreamingTSetImpl<T> extends BaseTSetWithSchema<T> implements
    StreamingTSet<T> {

  public StreamingTSetImpl(StreamingEnvironment tSetEnv, String name, int parallelism,
                           Schema inputSchema) {
    super(tSetEnv, name, parallelism, inputSchema);
  }

  @Override
  public StreamingEnvironment getTSetEnv() {
    return (StreamingEnvironment) super.getTSetEnv();
  }

  @Override
  public SDirectTLink<T> direct() {
    SDirectTLink<T> direct = new SDirectTLink<>(getTSetEnv(), getParallelism(),
        this.getOutputSchema());
    addChildToGraph(direct);
    return direct;
  }

  @Override
  public SReduceTLink<T> reduce(ReduceFunc<T> reduceFn) {
    SReduceTLink<T> reduce = new SReduceTLink<>(getTSetEnv(), reduceFn, getParallelism(),
        this.getOutputSchema());
    addChildToGraph(reduce);
    return reduce;
  }

  @Override
  public SPartitionTLink<T> partition(PartitionFunc<T> partitionFn, int targetParallelism) {
    SPartitionTLink<T> partition = new SPartitionTLink<>(getTSetEnv(),
        partitionFn, getParallelism(), targetParallelism, this.getOutputSchema());
    addChildToGraph(partition);
    return partition;
  }

  @Override
  public SPartitionTLink<T> partition(PartitionFunc<T> partitionFn) {
    return partition(partitionFn, getParallelism());
  }

  @Override
  public SGatherTLink<T> gather() {
    SGatherTLink<T> gather = new SGatherTLink<>(getTSetEnv(), getParallelism(),
        this.getOutputSchema());
    addChildToGraph(gather);
    return gather;
  }

  @Override
  public SAllReduceTLink<T> allReduce(ReduceFunc<T> reduceFn) {
    SAllReduceTLink<T> allreduce = new SAllReduceTLink<>(getTSetEnv(), reduceFn,
        getParallelism(), this.getOutputSchema());
    addChildToGraph(allreduce);
    return allreduce;
  }

  @Override
  public SAllGatherTLink<T> allGather() {
    SAllGatherTLink<T> allgather = new SAllGatherTLink<>(getTSetEnv(), getParallelism(),
        this.getOutputSchema());
    addChildToGraph(allgather);
    return allgather;
  }

  @Override
  public SComputeTSet<T> union(TSet<T> other) {

    if (this.getParallelism() != ((StreamingTSetImpl) other).getParallelism()) {
      throw new IllegalStateException("Parallelism of the TSets need to be the same in order to"
          + "perform a union operation");
    }

    SComputeTSet<T> union = direct().compute("sunion",
        new MapCompute<>((MapFunc<T, T>) input -> input));
    // now the following relationship is created
    // this -- directThis -- unionTSet

    SDirectTLink<T> directOther = new SDirectTLink<>(getTSetEnv(), getParallelism(),
        this.getOutputSchema());
    addChildToGraph(other, directOther);
    addChildToGraph(directOther, union);
    // now the following relationship is created
    // this __ directThis __ unionTSet
    // other __ directOther _/

    return union;
  }

  @Override
  public SComputeTSet<T> union(Collection<TSet<T>> tSets) {
    SComputeTSet<T> union = direct().compute("sunion",
        new MapCompute<>((MapFunc<T, T>) input -> input));
    // now the following relationship is created
    // this -- directThis -- unionTSet

    for (TSet<T> other : tSets) {
      if (this.getParallelism() != ((StreamingTSetImpl) other).getParallelism()) {
        throw new IllegalStateException("Parallelism of the TSets need to be the same in order to"
            + "perform a union operation");
      }
      SDirectTLink<T> directOther = new SDirectTLink<>(getTSetEnv(), getParallelism(),
          this.getOutputSchema());
      addChildToGraph(other, directOther);
      addChildToGraph(directOther, union);
    }
    return union;
  }

  @Override
  public <K, V> SKeyedTSet<K, V> mapToTuple(MapFunc<T, Tuple<K, V>> mapToTupleFn) {
    return direct().mapToTuple(mapToTupleFn);
//    throw new UnsupportedOperationException("Groupby is not avilable in streaming operations");
  }

  @Override
  public SReplicateTLink<T> replicate(int replications) {
    if (getParallelism() != 1) {
      throw new RuntimeException("Only tsets with parallelism 1 can be replicated: "
          + getParallelism());
    }

    SReplicateTLink<T> cloneTSet = new SReplicateTLink<>(getTSetEnv(), replications,
        this.getOutputSchema());
    addChildToGraph(cloneTSet);
    return cloneTSet;
  }

  @Override
  public StreamingTSetImpl<T> withSchema(Schema schema) {
    this.setOutputSchema(schema);
    return this;
  }
}
