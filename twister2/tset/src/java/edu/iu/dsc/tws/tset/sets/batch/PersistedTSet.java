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

import edu.iu.dsc.tws.api.comms.messaging.types.MessageType;
import edu.iu.dsc.tws.api.tset.fn.SinkFunc;
import edu.iu.dsc.tws.api.tset.sets.StorableTBase;
import edu.iu.dsc.tws.tset.env.BatchTSetEnvironment;

public class PersistedTSet<T> extends StoredTSet<T> {

  public PersistedTSet(BatchTSetEnvironment tSetEnv, SinkFunc<?> sinkFunc, int parallelism) {
    super(tSetEnv, "persisted", sinkFunc, parallelism);
  }

  @Override
  public PersistedTSet<T> persist() {
    return this;
  }

  @Override
  public PersistedTSet<T> lazyPersist() {
    return this;
  }

  @Override
  public CachedTSet<T> cache() {
    throw new UnsupportedOperationException("Cache on PersistedTSet is undefined!");
  }

  @Override
  public CachedTSet<T> lazyCache() {
    throw new UnsupportedOperationException("Cache on PersistedTSet is undefined!");
  }

  @Override
  public PersistedTSet<T> setName(String n) {
    rename(n);
    return this;
  }

  @Override
  public PersistedTSet<T> addInput(String key, StorableTBase<?> input) {
    return (PersistedTSet<T>) super.addInput(key, input);
  }

  public PersistedTSet<T> withDataType(MessageType dataType) {
    return (PersistedTSet<T>) super.withDataType(dataType);
  }
}
