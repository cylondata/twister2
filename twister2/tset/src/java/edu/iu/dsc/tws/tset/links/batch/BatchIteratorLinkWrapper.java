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

import edu.iu.dsc.tws.api.tset.sets.StorableTBase;
import edu.iu.dsc.tws.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.tset.sets.batch.CachedTSet;
import edu.iu.dsc.tws.tset.sets.batch.PersistedTSet;
import edu.iu.dsc.tws.tset.sinks.CacheIterSink;
import edu.iu.dsc.tws.tset.sinks.DiskPersistIterSink;

/**
 * Wrapper class for {@link BatchIteratorLink} that implements
 * {@link StorableTBase} related methods. Intended to be used with non-keyed
 * TLinks that would produce an {@link java.util.Iterator}
 *
 * @param <T> type
 */
public abstract class BatchIteratorLinkWrapper<T> extends BatchIteratorLink<T> {
  BatchIteratorLinkWrapper(BatchTSetEnvironment env, String n, int sourceP) {
    super(env, n, sourceP);
  }

  BatchIteratorLinkWrapper(BatchTSetEnvironment env, String n, int sourceP, int targetP) {
    super(env, n, sourceP, targetP);
  }

  @Override
  public CachedTSet<T> lazyCache() {
    CachedTSet<T> cacheTSet = new CachedTSet<>(getTSetEnv(), new CacheIterSink<T>(),
        getTargetParallelism());
    addChildToGraph(cacheTSet);

    return cacheTSet;
  }

  @Override
  public CachedTSet<T> cache() {
    return (CachedTSet<T>) super.cache();
  }

  @Override
  public PersistedTSet<T> lazyPersist() {
    PersistedTSet<T> persistedTSet = new PersistedTSet<>(getTSetEnv(),
        new DiskPersistIterSink<>(this.getId()), getTargetParallelism());
    addChildToGraph(persistedTSet);

    return persistedTSet;
  }

  @Override
  public PersistedTSet<T> persist() {
    return (PersistedTSet<T>) super.persist();
  }
}
