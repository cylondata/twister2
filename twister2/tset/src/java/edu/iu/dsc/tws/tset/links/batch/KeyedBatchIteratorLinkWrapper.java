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

import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.tset.sets.batch.KeyedCachedTSet;
import edu.iu.dsc.tws.tset.sinks.CacheIterSink;

public abstract class KeyedBatchIteratorLinkWrapper<K, V> extends BatchIteratorLink<Tuple<K, V>> {
  KeyedBatchIteratorLinkWrapper(BatchTSetEnvironment env, String n, int sourceP) {
    super(env, n, sourceP);
  }

  KeyedBatchIteratorLinkWrapper(BatchTSetEnvironment env, String n, int sourceP, int targetP) {
    super(env, n, sourceP, targetP);
  }

  @Override
  public KeyedCachedTSet<K, V> lazyCache() {
    KeyedCachedTSet<K, V> cacheTSet = new KeyedCachedTSet<>(getTSetEnv(), new CacheIterSink<>(),
        getTargetParallelism());
    addChildToGraph(cacheTSet);

    return cacheTSet;
  }

  @Override
  public KeyedCachedTSet<K, V> cache() {
    return (KeyedCachedTSet<K, V>) super.cache();
  }
}
