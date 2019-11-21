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
import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.compute.OperationNames;
import edu.iu.dsc.tws.api.compute.graph.Edge;
import edu.iu.dsc.tws.api.tset.fn.PartitionFunc;
import edu.iu.dsc.tws.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.tset.sets.batch.CachedTSet;

public class KeyedGatherUngroupedTLink<K, V> extends BatchIteratorLink<Tuple<K, V>> {
  private static final Logger LOG = Logger.getLogger(KeyedGatherUngroupedTLink.class.getName());

  private PartitionFunc<K> partitionFunction;
  private Comparator<K> keyCompartor;

  private boolean sortByKey;
  private boolean groupByKey = false;

  public KeyedGatherUngroupedTLink(BatchTSetEnvironment tSetEnv, int sourceParallelism) {
    this(tSetEnv, null, sourceParallelism, false, null);
  }

  public KeyedGatherUngroupedTLink(BatchTSetEnvironment tSetEnv, PartitionFunc<K> partitionFn,
                                   int sourceParallelism) {
    this(tSetEnv, partitionFn, sourceParallelism, false, null);
  }

  public KeyedGatherUngroupedTLink(BatchTSetEnvironment tSetEnv, PartitionFunc<K> partitionFn,
                                   int sourceParallelism, boolean sortByKey,
                                   Comparator<K> keyCompartor) {
    super(tSetEnv, "kgather", sourceParallelism);
    this.partitionFunction = partitionFn;
    this.sortByKey = sortByKey;
    this.keyCompartor = keyCompartor;
    if (sortByKey && keyCompartor == null) {
      LOG.warning("Key comparator cannot be null when sorting is true");
    }
  }

  @Override
  public Edge getEdge() {
    Edge e = new Edge(getId(), OperationNames.KEYED_GATHER, getMessageType());
    e.setKeyed(true);
    e.setPartitioner(partitionFunction);
    e.addProperty(CommunicationContext.SORT_BY_KEY, this.sortByKey);
    e.addProperty(CommunicationContext.GROUP_BY_KEY, this.groupByKey);

    if (this.keyCompartor != null) {
      e.addProperty(CommunicationContext.KEY_COMPARATOR, this.keyCompartor);
    }
    return e;
  }

  void enableGroupByKey() {
    this.groupByKey = true;
  }

  @Override
  public KeyedGatherUngroupedTLink<K, V> setName(String n) {
    rename(n);
    return this;
  }

  @Override
  public CachedTSet<Tuple<K, V>> lazyCache() {
    return (CachedTSet<Tuple<K, V>>) super.lazyCache();
  }

  @Override
  public CachedTSet<Tuple<K, V>> cache() {
    return (CachedTSet<Tuple<K, V>>) super.cache();
  }
}
