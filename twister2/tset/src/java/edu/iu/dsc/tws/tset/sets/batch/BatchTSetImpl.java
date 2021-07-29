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

package edu.iu.dsc.tws.tset.sets.batch;

import java.util.Collection;

import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.tset.fn.MapFunc;
import edu.iu.dsc.tws.api.tset.fn.PartitionFunc;
import edu.iu.dsc.tws.api.tset.fn.ReduceFunc;
import edu.iu.dsc.tws.api.tset.schema.Schema;
import edu.iu.dsc.tws.api.tset.sets.StorableTBase;
import edu.iu.dsc.tws.api.tset.sets.TSet;
import edu.iu.dsc.tws.api.tset.sets.batch.BatchTSet;
import edu.iu.dsc.tws.tset.env.BatchEnvironment;
import edu.iu.dsc.tws.tset.fn.MapIterCompute;
import edu.iu.dsc.tws.tset.links.batch.AllGatherTLink;
import edu.iu.dsc.tws.tset.links.batch.AllReduceTLink;
import edu.iu.dsc.tws.tset.links.batch.DirectTLink;
import edu.iu.dsc.tws.tset.links.batch.GatherTLink;
import edu.iu.dsc.tws.tset.links.batch.PartitionTLink;
import edu.iu.dsc.tws.tset.links.batch.PipeTLink;
import edu.iu.dsc.tws.tset.links.batch.ReduceTLink;
import edu.iu.dsc.tws.tset.links.batch.ReplicateTLink;
import edu.iu.dsc.tws.tset.sets.BaseTSetWithSchema;
import edu.iu.dsc.tws.tset.sets.batch.functions.IdentityFunction;

public abstract class BatchTSetImpl<T> extends BaseTSetWithSchema<T> implements BatchTSet<T> {

  public BatchTSetImpl() {
    //non arg constructor needed for kryo
  }

  /**
   * General constructor for batch {@link TSet}s
   *
   * @param tSetEnv     env
   * @param name        name
   * @param parallelism par
   * @param inputSchema Schema from the preceding {@link edu.iu.dsc.tws.api.tset.link.TLink}
   */
  BatchTSetImpl(BatchEnvironment tSetEnv, String name, int parallelism, Schema inputSchema) {
    super(tSetEnv, name, parallelism, inputSchema);
  }

  @Override
  public BatchEnvironment getTSetEnv() {
    return (BatchEnvironment) super.getTSetEnv();
  }

  @Override
  public DirectTLink<T> direct() {
    DirectTLink<T> direct = new DirectTLink<>(getTSetEnv(), getParallelism(), getOutputSchema());
    addChildToGraph(direct);
    return direct;
  }

  public PipeTLink<T> pipe() {
    PipeTLink<T> pipe = new PipeTLink<>(getTSetEnv(), getParallelism(), getOutputSchema());
    addChildToGraph(pipe);
    return pipe;
  }

  @Override
  public ReduceTLink<T> reduce(ReduceFunc<T> reduceFn) {
    ReduceTLink<T> reduce = new ReduceTLink<>(getTSetEnv(), reduceFn, getParallelism(),
        getOutputSchema());
    addChildToGraph(reduce);
    return reduce;
  }

  @Override
  public PartitionTLink<T> partition(PartitionFunc<T> partitionFn, int targetParallelism) {
    PartitionTLink<T> partition = new PartitionTLink<>(getTSetEnv(), partitionFn, getParallelism(),
        targetParallelism, getOutputSchema());
    addChildToGraph(partition);
    return partition;
  }

  @Override
  public PartitionTLink<T> partition(PartitionFunc<T> partitionFn) {
    return partition(partitionFn, getParallelism());
  }

  @Override
  public GatherTLink<T> gather() {
    GatherTLink<T> gather = new GatherTLink<>(getTSetEnv(), getParallelism(), getOutputSchema());
    addChildToGraph(gather);
    return gather;
  }

  @Override
  public AllReduceTLink<T> allReduce(ReduceFunc<T> reduceFn) {
    AllReduceTLink<T> reduce = new AllReduceTLink<>(getTSetEnv(), reduceFn, getParallelism(),
        getOutputSchema());
    addChildToGraph(reduce);
    return reduce;
  }

  @Override
  public AllGatherTLink<T> allGather() {
    AllGatherTLink<T> gather = new AllGatherTLink<>(getTSetEnv(), getParallelism(),
        getOutputSchema());
    addChildToGraph(gather);
    return gather;
  }

  // todo: remove this direct() --> would be more efficient. can handle at the context write level
  @Override
  public <K, V> KeyedTSet<K, V> mapToTuple(MapFunc<T, Tuple<K, V>> mapToTupleFn) {
    return pipe().mapToTuple(mapToTupleFn);
  }

  @Override
  public ComputeTSet<T> union(TSet<T> other) {

    if (this.getParallelism() != ((BatchTSetImpl) other).getParallelism()) {
      throw new IllegalStateException("Parallelism of the TSets need to be the same in order to"
          + "perform a union operation");
    }

    ComputeTSet<T> unionTSet = direct().compute("union",
        new MapIterCompute<>(new IdentityFunction<>()));
    // now the following relationship is created
    // this -- directThis -- unionTSet

    DirectTLink<T> directOther = new DirectTLink<>(getTSetEnv(), getParallelism(),
        getOutputSchema());
    addChildToGraph(other, directOther);
    addChildToGraph(directOther, unionTSet);
    // now the following relationship is created
    // this __ directThis __ unionTSet
    // other __ directOther _/

    return unionTSet;
  }

  @Override
  public ComputeTSet<T> union(Collection<TSet<T>> tSets) {

    ComputeTSet<T> unionTSet = direct().compute("union",
        new MapIterCompute<>(new IdentityFunction<>()));
    // now the following relationship is created
    // this -- directThis -- unionTSet

    for (TSet<T> tSet : tSets) {
      if (this.getParallelism() != ((BatchTSetImpl) tSet).getParallelism()) {
        throw new IllegalStateException("Parallelism of the TSets need to be the same in order to"
            + "perform a union operation");
      }
      DirectTLink<T> directOther = new DirectTLink<>(getTSetEnv(), getParallelism(),
          getOutputSchema());
      addChildToGraph(tSet, directOther);
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

    ReplicateTLink<T> cloneTSet = new ReplicateTLink<>(getTSetEnv(), replications,
        getOutputSchema());
    addChildToGraph(cloneTSet);
    return cloneTSet;
  }

  @Override
  public CachedTSet<T> cache() {
    // todo remove this direct and plug it into the underlying tset op
    return direct().cache();
  }

  @Override
  public CachedTSet<T> lazyCache() {
    return direct().lazyCache();
  }

  @Override
  public PersistedTSet<T> persist() {
    return direct().persist();
  }

  @Override
  public PersistedTSet<T> lazyPersist() {
    return direct().lazyPersist();
  }

  @Override
  public BatchTSetImpl<T> addInput(String key, StorableTBase<?> input) {
    getTSetEnv().addInput(getId(), input.getId(), key);
    return this;
  }

  @Override
  public BatchTSetImpl<T> withSchema(Schema schema) {
    this.setOutputSchema(schema);
    return this;
  }
}
