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

import java.util.Iterator;

import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.tset.fn.SinkFunc;
import edu.iu.dsc.tws.tset.env.BatchTSetEnvironment;

public class KeyedCachedTSet<K, V> extends KeyedStoredTSet<K, V> {

  public KeyedCachedTSet(BatchTSetEnvironment tSetEnv, SinkFunc<Iterator<Tuple<K, V>>> sinkFunc,
                         int parallelism) {
    super(tSetEnv, "kcached", sinkFunc, parallelism);
  }

  @Override
  public KeyedCachedTSet<K, V> setName(String n) {
    return (KeyedCachedTSet<K, V>) super.setName(n);
  }

  @Override
  public KeyedCachedTSet<K, V> cache() {
    return this;
  }

  @Override
  public KeyedCachedTSet<K, V> lazyCache() {
    return this;
  }

  @Override
  public KeyedPersistedTSet<K, V> persist() {
    throw new UnsupportedOperationException("persist on CachedTSet is undefined!");
  }

  @Override
  public KeyedPersistedTSet<K, V> lazyPersist() {
    throw new UnsupportedOperationException("lazyPersist on CachedTSet is undefined!");
  }
}
