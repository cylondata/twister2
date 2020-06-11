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

package edu.iu.dsc.tws.tset.sinks;

import java.util.Iterator;

import edu.iu.dsc.tws.api.tset.fn.BaseSinkFunc;
import edu.iu.dsc.tws.dataset.partition.CollectionPartition;

/**
 * Base classed used by cache/persist sinks that receive an {@link Iterator}.
 *
 * @param <T>  Base type of the input
 * @param <T1> type that is passed to the partition through the {@link StoreIterSink}.extractValue
 *             method
 */
public abstract class StoreIterSink<T, T1> extends BaseSinkFunc<Iterator<T>> {

  /**
   * Extracts value from an input value of the iterator
   */
  protected abstract T1 extractValue(T input);

  /**
   * Returns the {@link CollectionPartition} that would accept the values. This could be an in-mem
   * collection or a disk-backed collection
   *
   * @return collection partition
   */
  public abstract CollectionPartition<T1> get();

  @Override
  public boolean add(Iterator<T> value) {
    while (value.hasNext()) {
      this.get().add(extractValue(value.next()));
    }
    return true;
  }
}
