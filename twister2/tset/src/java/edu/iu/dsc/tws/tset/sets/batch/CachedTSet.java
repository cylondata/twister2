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

import edu.iu.dsc.tws.api.tset.fn.SinkFunc;
import edu.iu.dsc.tws.api.tset.schema.Schema;
import edu.iu.dsc.tws.api.tset.sets.StorableTBase;
import edu.iu.dsc.tws.tset.env.BatchEnvironment;

/**
 * Cached tset
 *
 * @param <T> base type of the tset
 */
public class CachedTSet<T> extends StoredTSet<T> {
  /*
  Sink function type is unknown as we need to preserve the output datao bject type to T. In doing
  so, we would need to have several types of sink functions that can convert the comms message to
   T. example: for direct, sink func would convert Iterator<T> to T.
   */
  public CachedTSet(BatchEnvironment tSetEnv, SinkFunc<?> sinkFunc, int parallelism,
                    Schema inputSchema) {
    super(tSetEnv, "cached", sinkFunc, parallelism, inputSchema);
  }

  @Override
  public CachedTSet<T> cache() {
    return this;
  }

  @Override
  public CachedTSet<T> lazyCache() {
    return this;
  }

  @Override
  public PersistedTSet<T> persist() {
    throw new UnsupportedOperationException("persist on CachedTSet is undefined!");
  }

  @Override
  public PersistedTSet<T> lazyPersist() {
    throw new UnsupportedOperationException("lazyPersist on CachedTSet is undefined!");
  }

  @Override
  public CachedTSet<T> setName(String n) {
    rename(n);
    return this;
  }

  @Override
  public CachedTSet<T> addInput(String key, StorableTBase<?> input) {
    return (CachedTSet<T>) super.addInput(key, input);
  }

  @Override
  public CachedTSet<T> withSchema(Schema schema) {
    return (CachedTSet<T>) super.withSchema(schema);
  }
}
