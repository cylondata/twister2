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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import edu.iu.dsc.tws.api.comms.CommunicationContext;
import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.compute.TaskPartitioner;
import edu.iu.dsc.tws.api.compute.nodes.INode;
import edu.iu.dsc.tws.api.dataset.DataObject;
import edu.iu.dsc.tws.api.dataset.DataPartition;
import edu.iu.dsc.tws.api.dataset.DataPartitionConsumer;
import edu.iu.dsc.tws.api.tset.Storable;
import edu.iu.dsc.tws.api.tset.fn.PartitionFunc;
import edu.iu.dsc.tws.api.tset.fn.ReduceFunc;
import edu.iu.dsc.tws.api.tset.fn.SinkFunc;
import edu.iu.dsc.tws.api.tset.sets.batch.BatchTupleTSet;
import edu.iu.dsc.tws.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.tset.links.batch.JoinTLink;
import edu.iu.dsc.tws.tset.links.batch.KeyedDirectTLink;
import edu.iu.dsc.tws.tset.links.batch.KeyedGatherTLink;
import edu.iu.dsc.tws.tset.links.batch.KeyedGatherUngroupedTLink;
import edu.iu.dsc.tws.tset.links.batch.KeyedPartitionTLink;
import edu.iu.dsc.tws.tset.links.batch.KeyedReduceTLink;
import edu.iu.dsc.tws.tset.ops.SinkOp;
import edu.iu.dsc.tws.tset.sources.DataPartitionSourceFunc;

public class KeyedStoredTSet<K, V> extends BatchTupleTSetImpl<K, V> implements
    Storable<Tuple<K, V>> {
  // keyed comms always return an iterator of tuples
  private SinkFunc<Iterator<Tuple<K, V>>> storingSinkFunc;
  private String storedSourcePrefix;

  protected KeyedSourceTSet<K, V> storedSource;

  KeyedStoredTSet(BatchTSetEnvironment tSetEnv, String name,
                  SinkFunc<Iterator<Tuple<K, V>>> sinkFunc, int parallelism) {
    super(tSetEnv, name, parallelism);
    this.storingSinkFunc = sinkFunc;
    this.storedSourcePrefix = "kstored(" + getId() + ")";
  }

  // implement buildable TSet methods
  @Override
  public INode getINode() {
    return new SinkOp<>(storingSinkFunc, this, getInputs());
  }

  // implement storable methods
  @Override
  public List<Tuple<K, V>> getData() {
    List<Tuple<K, V>> results = new ArrayList<>();

    DataObject<Tuple<K, V>> data = getTSetEnv().getData(getId());

    if (data != null) {
      for (DataPartition<Tuple<K, V>> partition : data.getPartitions()) {
        DataPartitionConsumer<Tuple<K, V>> consumer = partition.getConsumer();
        while (consumer.hasNext()) {
          results.add(consumer.next());
        }
      }
    }

    return results;
  }

  @Override
  public KeyedSourceTSet<K, V> getStoredSourceTSet() {
    if (this.storedSource == null) {
      // this cache source will consume the data object created by the execution of this tset.
      // hence this tset ID needs to be set as an input to the cache source
      this.storedSource = getTSetEnv().createKeyedSource(storedSourcePrefix,
          new DataPartitionSourceFunc<>(storedSourcePrefix), getParallelism());
      this.storedSource.addInput(storedSourcePrefix, this);
    }

    return this.storedSource;
  }

  // override tuple TSet methods
  @Override
  public KeyedDirectTLink<K, V> keyedDirect() {
    return getStoredSourceTSet().keyedDirect();
  }

  @Override
  public KeyedReduceTLink<K, V> keyedReduce(ReduceFunc<V> reduceFn) {
    return getStoredSourceTSet().keyedReduce(reduceFn);
  }

  @Override
  public KeyedPartitionTLink<K, V> keyedPartition(PartitionFunc<K> partitionFn) {
    return getStoredSourceTSet().keyedPartition(partitionFn);
  }

  @Override
  public KeyedGatherTLink<K, V> keyedGather() {
    return getStoredSourceTSet().keyedGather();
  }

  @Override
  public KeyedGatherTLink<K, V> keyedGather(PartitionFunc<K> partitionFn) {
    return getStoredSourceTSet().keyedGather(partitionFn);
  }

  @Override
  public KeyedGatherTLink<K, V> keyedGather(PartitionFunc<K> partitionFn,
                                            Comparator<K> comparator) {
    return getStoredSourceTSet().keyedGather(partitionFn, comparator);
  }

  @Override
  public KeyedGatherUngroupedTLink<K, V> keyedGatherUngrouped() {
    return getStoredSourceTSet().keyedGatherUngrouped();
  }

  @Override
  public KeyedGatherUngroupedTLink<K, V> keyedGatherUngrouped(PartitionFunc<K> partitionFn) {
    return getStoredSourceTSet().keyedGatherUngrouped(partitionFn);
  }

  @Override
  public KeyedGatherUngroupedTLink<K, V> keyedGatherUngrouped(PartitionFunc<K> partitionFn,
                                                              Comparator<K> comparator) {
    return getStoredSourceTSet().keyedGatherUngrouped(partitionFn, comparator);
  }

  @Override
  public <VR> JoinTLink<K, V, VR> join(BatchTupleTSet<K, VR> rightTSet,
                                       CommunicationContext.JoinType type,
                                       Comparator<K> keyComparator,
                                       TaskPartitioner<K> partitioner) {
    return getStoredSourceTSet().join(rightTSet, type, keyComparator, partitioner);
  }

  @Override
  public <VR> JoinTLink<K, V, VR> join(BatchTupleTSet<K, VR> rightTSet,
                                       CommunicationContext.JoinType type,
                                       Comparator<K> keyComparator) {
    return getStoredSourceTSet().join(rightTSet, type, keyComparator);
  }
}
