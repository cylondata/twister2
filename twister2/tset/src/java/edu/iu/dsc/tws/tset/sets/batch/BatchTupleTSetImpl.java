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

import java.util.Comparator;

import edu.iu.dsc.tws.api.comms.CommunicationContext;
import edu.iu.dsc.tws.api.compute.TaskPartitioner;
import edu.iu.dsc.tws.api.tset.fn.PartitionFunc;
import edu.iu.dsc.tws.api.tset.fn.ReduceFunc;
import edu.iu.dsc.tws.api.tset.schema.JoinSchema;
import edu.iu.dsc.tws.api.tset.schema.KeyedSchema;
import edu.iu.dsc.tws.api.tset.schema.PrimitiveSchemas;
import edu.iu.dsc.tws.api.tset.schema.Schema;
import edu.iu.dsc.tws.api.tset.schema.TupleSchema;
import edu.iu.dsc.tws.api.tset.sets.StorableTBase;
import edu.iu.dsc.tws.api.tset.sets.batch.BatchTupleTSet;
import edu.iu.dsc.tws.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.tset.links.batch.JoinTLink;
import edu.iu.dsc.tws.tset.links.batch.KeyedDirectTLink;
import edu.iu.dsc.tws.tset.links.batch.KeyedGatherTLink;
import edu.iu.dsc.tws.tset.links.batch.KeyedGatherUngroupedTLink;
import edu.iu.dsc.tws.tset.links.batch.KeyedPartitionTLink;
import edu.iu.dsc.tws.tset.links.batch.KeyedReduceTLink;
import edu.iu.dsc.tws.tset.sets.BaseTSetWithSchema;

/**
 * Attaches a key to the oncoming data.
 *
 * @param <K> key type
 * @param <V> data (value) type
 */
public abstract class BatchTupleTSetImpl<K, V> extends BaseTSetWithSchema<V> implements
    BatchTupleTSet<K, V> {

  /**
   * General constructor for batch {@link edu.iu.dsc.tws.api.tset.sets.TupleTSet}s
   *
   * @param tSetEnv     env
   * @param name        name
   * @param parallelism par
   * @param inputSchema Schema from the preceding {@link edu.iu.dsc.tws.api.tset.link.TLink}
   */
  BatchTupleTSetImpl(BatchTSetEnvironment tSetEnv, String name, int parallelism,
                     Schema inputSchema) {
    // since the output schema will be a KeyedSchema, it needs to be initialized by a KeyedSchema
    // of OBJECT type
    super(tSetEnv, name, parallelism, inputSchema, PrimitiveSchemas.OBJECT_TUPLE2);
  }

  protected BatchTupleTSetImpl() {
    //non arg constructor for kryo
  }

  // since keyed tset is the base impl for BatchTupleTSet, it needs to override the env getter
  @Override
  public BatchTSetEnvironment getTSetEnv() {
    return (BatchTSetEnvironment) super.getTSetEnv();
  }

  @Override
  public KeyedDirectTLink<K, V> keyedDirect() {
    KeyedDirectTLink<K, V> kDirect = new KeyedDirectTLink<>(getTSetEnv(), getParallelism(),
        getOutputSchema());
    addChildToGraph(kDirect);
    return kDirect;
  }

  @Override
  public KeyedReduceTLink<K, V> keyedReduce(ReduceFunc<V> reduceFn) {
    KeyedReduceTLink<K, V> reduce = new KeyedReduceTLink<>(getTSetEnv(), reduceFn,
        getParallelism(), getOutputSchema());
    addChildToGraph(reduce);
    return reduce;
  }

  @Override
  public KeyedPartitionTLink<K, V> keyedPartition(PartitionFunc<K> partitionFn) {
    KeyedPartitionTLink<K, V> partition = new KeyedPartitionTLink<>(getTSetEnv(), partitionFn,
        getParallelism(), getOutputSchema());
    addChildToGraph(partition);
    return partition;
  }

  @Override
  public KeyedGatherTLink<K, V> keyedGather() {
    KeyedGatherTLink<K, V> gather = new KeyedGatherTLink<>(getTSetEnv(), getParallelism(),
        getOutputSchema());
    addChildToGraph(gather);
    return gather;
  }

  @Override
  public KeyedGatherTLink<K, V> keyedGather(PartitionFunc<K> partitionFn) {
    KeyedGatherTLink<K, V> gather = new KeyedGatherTLink<>(getTSetEnv(),
        partitionFn, getParallelism(), getOutputSchema());
    addChildToGraph(gather);
    return gather;
  }

  @Override
  public KeyedGatherTLink<K, V> keyedGather(PartitionFunc<K> partitionFn,
                                            Comparator<K> comparator) {
    KeyedGatherTLink<K, V> gather = new KeyedGatherTLink<>(getTSetEnv(), partitionFn,
        getParallelism(), comparator, getOutputSchema());
    addChildToGraph(gather);
    return gather;
  }

  @Override
  public KeyedGatherUngroupedTLink<K, V> keyedGatherUngrouped() {
    KeyedGatherUngroupedTLink<K, V> gather = new KeyedGatherUngroupedTLink<>(getTSetEnv(),
        getParallelism(), getOutputSchema());
    addChildToGraph(gather);
    return gather;
  }

  @Override
  public KeyedGatherUngroupedTLink<K, V> keyedGatherUngrouped(PartitionFunc<K> partitionFn) {
    KeyedGatherUngroupedTLink<K, V> gather = new KeyedGatherUngroupedTLink<>(getTSetEnv(),
        partitionFn, getParallelism(), getOutputSchema());
    addChildToGraph(gather);
    return gather;
  }

  @Override
  public KeyedGatherUngroupedTLink<K, V> keyedGatherUngrouped(PartitionFunc<K> partitionFn,
                                                              Comparator<K> comparator) {
    KeyedGatherUngroupedTLink<K, V> gather = new KeyedGatherUngroupedTLink<>(getTSetEnv(),
        partitionFn, getParallelism(), comparator, getOutputSchema());
    addChildToGraph(gather);
    return gather;
  }

  @Override
  public <VR> JoinTLink<K, V, VR> join(BatchTupleTSet<K, VR> rightTSet,
                                       CommunicationContext.JoinType type,
                                       Comparator<K> keyComparator,
                                       TaskPartitioner<K> partitioner) {
    JoinTLink<K, V, VR> join;

    JoinSchema joinSchema = new JoinSchema(this.getOutputSchema(),
        ((BatchTupleTSetImpl<K, VR>) rightTSet).getOutputSchema());

    if (partitioner != null) {
      join = new JoinTLink<>(getTSetEnv(), type, keyComparator, partitioner, this, rightTSet,
          joinSchema);
    } else {
      join = new JoinTLink<>(getTSetEnv(), type, keyComparator, this, rightTSet,
          joinSchema);
    }
    addChildToGraph(join);

    // add the right tset connection
    getTSetEnv().getGraph().connectTSets(rightTSet, join);

    return join;
  }

  @Override
  public <VR> JoinTLink<K, V, VR> join(BatchTupleTSet<K, VR> rightTSet,
                                       CommunicationContext.JoinType type,
                                       Comparator<K> keyComparator) {
    return join(rightTSet, type, keyComparator, null);
  }

  @Override
  public BatchTupleTSetImpl<K, V> setName(String n) {
    rename(n);
    return this;
  }

  @Override
  public KeyedCachedTSet<K, V> cache() {
    return keyedDirect().cache();
  }

  @Override
  public KeyedCachedTSet<K, V> lazyCache() {
    return keyedDirect().lazyCache();
  }

  @Override
  public KeyedPersistedTSet<K, V> persist() {
    return keyedDirect().persist();
  }

  @Override
  public KeyedPersistedTSet<K, V> lazyPersist() {
    return null;
  }

  @Override
  public BatchTupleTSetImpl<K, V> addInput(String key, StorableTBase<?> input) {
    getTSetEnv().addInput(getId(), input.getId(), key);
    return this;
  }

  @Override
  public BatchTupleTSetImpl<K, V> withSchema(TupleSchema schema) {
    this.setOutputSchema(schema);
    return this;
  }

  /**
   * Output schema of a BatchTupleTSet is definitely a {@link KeyedSchema}.
   *
   * @return keyed schema
   */
  public KeyedSchema getOutputSchema() {
    return (KeyedSchema) super.getOutputSchema();
  }
}
