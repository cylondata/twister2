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

import edu.iu.dsc.tws.api.comms.CommunicationContext;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.compute.OperationNames;
import edu.iu.dsc.tws.api.compute.graph.Edge;
import edu.iu.dsc.tws.api.tset.fn.MapFunc;
import edu.iu.dsc.tws.api.tset.schema.TupleSchema;
import edu.iu.dsc.tws.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.tset.links.TLinkUtils;
import edu.iu.dsc.tws.tset.sets.batch.KeyedTSet;

public class KeyedDirectTLink<K, V> extends KeyedBatchIteratorLinkWrapper<K, V> {
  private boolean useDisk = false;

  public KeyedDirectTLink(BatchTSetEnvironment tSetEnv, int sourceParallelism,
                          TupleSchema schema) {
    super(tSetEnv, "kdirect", sourceParallelism, schema);
  }

  public KeyedTSet<K, V> mapToTuple() {
    return super.mapToTuple((MapFunc<Tuple<K, V>, Tuple<K, V>>) input -> input);
  }

  @Override
  public KeyedDirectTLink<K, V> setName(String name) {
    rename(name);
    return this;
  }

  @Override
  public Edge getEdge() {
    // NOTE: There is no keyed direct in the communication layer!!! Hence this is an keyed direct
    // emulation. Therefore, we can not use user provided data types here because we will be using
    // Tuple<K, V> object through a DirectLink here.
    // todo fix this ambiguity!
    Edge e = new Edge(getId(), OperationNames.DIRECT, MessageTypes.OBJECT);
    e.addProperty(CommunicationContext.USE_DISK, this.useDisk);
    TLinkUtils.generateKeyedCommsSchema(getSchema(), e);
    return e;
  }

  public KeyedDirectTLink<K, V> useDisk() {
    this.useDisk = true;
    return this;
  }

//  @Override
//  public KeyedCachedTSet<K, V> lazyCache() {
//    KeyedCachedTSet<K, V> cacheTSet = new KeyedCachedTSet<>(getTSetEnv(), new CacheIterSink<>(),
//        getTargetParallelism());
//    addChildToGraph(cacheTSet);
//
//    return cacheTSet;
//  }
//
//  @Override
//  public KeyedCachedTSet<K, V> cache() {
//    KeyedCachedTSet<K, V> cacheTSet = lazyCache();
//    getTSetEnv().run(cacheTSet);
//    return cacheTSet;
//  }
}
