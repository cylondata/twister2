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

import edu.iu.dsc.tws.api.tset.fn.PartitionFunc;
import edu.iu.dsc.tws.api.tset.schema.TupleSchema;
import edu.iu.dsc.tws.tset.env.BatchEnvironment;

public class KeyedGatherTLink<K, V> extends KeyedGatherUngroupedTLink<K, Iterator<V>> {
  private static final Logger LOG = Logger.getLogger(KeyedGatherTLink.class.getName());

  private KeyedGatherTLink() {
    //non arg constructor for kryo
  }

  public KeyedGatherTLink(BatchEnvironment tSetEnv, int sourceParallelism, TupleSchema schema) {
    this(tSetEnv, null, sourceParallelism, null, schema);
  }

  public KeyedGatherTLink(BatchEnvironment tSetEnv, PartitionFunc<K> partitionFn,
                          int sourceParallelism, TupleSchema schema) {
    this(tSetEnv, partitionFn, sourceParallelism, null, schema);
  }

  public KeyedGatherTLink(BatchEnvironment tSetEnv, PartitionFunc<K> partitionFn,
                          int sourceParallelism, Comparator<K> keyCompartor, TupleSchema schema) {
    super(tSetEnv, partitionFn, sourceParallelism, keyCompartor, schema);
    this.enableGroupByKey();
  }

  @Override
  public KeyedGatherTLink<K, V> useDisk() {
    super.useDisk();
    return this;
  }

  @Override
  public KeyedGatherTLink<K, V> setName(String n) {
    return (KeyedGatherTLink<K, V>) super.setName(n);
  }
}
