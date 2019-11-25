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
package edu.iu.dsc.tws.api.tset.sets;

/**
 * Adds the capability to to store data for {@link TSet}s.
 *
 * @param <T> tset type
 */
public interface StoringData<T> {

  /**
   * Runs this TSet and caches the data to an in-memory
   * {@link edu.iu.dsc.tws.api.dataset.DataPartition} and exposes the data as another TSet.
   *
   * @return Cached TSet
   */
  StorableTBase<T> cache();

  /**
   * Performs caching lazily. i.e. cache operation would only be performed when the {@link TSet}
   * is evaluated explicitly.
   *
   * @return Cached TSet
   */
  StorableTBase<T> lazyCache();

  /**
   * Similar to cache, but the data is stored in a disk based
   * {@link edu.iu.dsc.tws.api.dataset.DataPartition}. This method would also expose the
   * checkpointing ability to {@link TSet}s.
   *
   * @return Persisted / Checkpointed TSets
   */
  StorableTBase<T> persist();

  /**
   * Performs persisting lazily.
   *
   * @return Persisted / Checkpointed TSets
   */
  StorableTBase<T> lazyPersist();
}
