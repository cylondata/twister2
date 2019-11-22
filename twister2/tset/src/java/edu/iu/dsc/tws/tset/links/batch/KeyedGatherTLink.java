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
import java.util.Iterator;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.tset.fn.PartitionFunc;
import edu.iu.dsc.tws.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.tset.sets.batch.CachedTSet;

public class KeyedGatherTLink<K, V> extends KeyedGatherUngroupedTLink<K, Iterator<V>> {
  private static final Logger LOG = Logger.getLogger(KeyedGatherTLink.class.getName());

  public KeyedGatherTLink(BatchTSetEnvironment tSetEnv, int sourceParallelism) {
    this(tSetEnv, null, sourceParallelism, false, null);
  }

  public KeyedGatherTLink(BatchTSetEnvironment tSetEnv, PartitionFunc<K> partitionFn,
                          int sourceParallelism) {
    this(tSetEnv, partitionFn, sourceParallelism, false, null);
  }

  public KeyedGatherTLink(BatchTSetEnvironment tSetEnv, PartitionFunc<K> partitionFn,
                          int sourceParallelism, boolean sortByKey, Comparator<K> keyCompartor) {
    super(tSetEnv, partitionFn, sourceParallelism, sortByKey, keyCompartor);
    this.enableGroupByKey();
  }

  @Override
  public KeyedGatherTLink<K, V> setName(String n) {
    return (KeyedGatherTLink<K, V>) super.setName(n);
  }
}
