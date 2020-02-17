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

package edu.iu.dsc.tws.tset.links.batch;

import java.util.Comparator;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.comms.CommunicationContext;
import edu.iu.dsc.tws.api.compute.OperationNames;
import edu.iu.dsc.tws.api.compute.graph.Edge;
import edu.iu.dsc.tws.api.tset.fn.PartitionFunc;
import edu.iu.dsc.tws.api.tset.schema.TupleSchema;
import edu.iu.dsc.tws.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.tset.links.TLinkUtils;

public class KeyedGatherUngroupedTLink<K, V> extends KeyedBatchIteratorLinkWrapper<K, V> {
  private static final Logger LOG = Logger.getLogger(KeyedGatherUngroupedTLink.class.getName());

  private PartitionFunc<K> partitionFunction;
  private Comparator<K> keyCompartor;

  private boolean groupByKey = false;

  private boolean useDisk = false;

  public KeyedGatherUngroupedTLink(BatchTSetEnvironment tSetEnv, int sourceParallelism,
                                   TupleSchema schema) {
    this(tSetEnv, null, sourceParallelism, null, schema);
  }

  public KeyedGatherUngroupedTLink(BatchTSetEnvironment tSetEnv, PartitionFunc<K> partitionFn,
                                   int sourceParallelism, TupleSchema schema) {
    this(tSetEnv, partitionFn, sourceParallelism, null, schema);
  }

  public KeyedGatherUngroupedTLink(BatchTSetEnvironment tSetEnv, PartitionFunc<K> partitionFn,
                                   int sourceParallelism, Comparator<K> keyCompartor,
                                   TupleSchema schema) {
    super(tSetEnv, "kgather", sourceParallelism, schema);
    this.partitionFunction = partitionFn;
    this.keyCompartor = keyCompartor;
  }

  public KeyedGatherUngroupedTLink() {
    //non arg constructor for kryo
    LOG.warning("This constructors should only be used by kryo");
  }

  @Override
  public Edge getEdge() {
    Edge e = new Edge(getId(), OperationNames.KEYED_GATHER, this.getSchema().getDataType());
    e.setKeyed(true);
    e.setKeyType(this.getSchema().getKeyType());
    e.setPartitioner(partitionFunction);
    e.addProperty(CommunicationContext.SORT_BY_KEY, this.keyCompartor != null);
    e.addProperty(CommunicationContext.GROUP_BY_KEY, this.groupByKey);

    if (this.keyCompartor != null) {
      e.addProperty(CommunicationContext.KEY_COMPARATOR, this.keyCompartor);
    }
    e.addProperty(CommunicationContext.USE_DISK, this.useDisk);
    TLinkUtils.generateKeyedCommsSchema(getSchema(), e);
    return e;
  }

  void enableGroupByKey() {
    this.groupByKey = true;
  }

  public KeyedGatherUngroupedTLink<K, V> useDisk() {
    this.useDisk = true;
    return this;
  }

  @Override
  public KeyedGatherUngroupedTLink<K, V> setName(String n) {
    rename(n);
    return this;
  }

//  @Override
//  public CachedTSet<Tuple<K, V>> lazyCache() {
//    return (CachedTSet<Tuple<K, V>>) super.lazyCache();
//  }
//
//  @Override
//  public CachedTSet<Tuple<K, V>> cache() {
//    return (CachedTSet<Tuple<K, V>>) super.cache();
//  }
}
