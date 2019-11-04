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
 * Base classed used by cache/persist sinks that receive an {@link Iterator<T>}.
 *
 * @param <T>  Base type of the input
 * @param <T1> type that is passed to the partition through the {@link ValueExtractor}
 */
public abstract class StoreIterSink<T, T1> extends
    BaseSinkFunc<Iterator<T>> {

  /**
   * Returns the {@link CollectionPartition} that would accept the values. This could be an in-mem
   * collection or a disk-backed collection
   *
   * @return collection partition
   */
  protected abstract CollectionPartition<T1> getPartition();

  /**
   * A map function that would extract value type T1 from input type T
   *
   * @return value extractor
   */
  protected abstract ValueExtractor<T1, T> getValueExtractor();

  /**
   * Extracts value from an input
   * @param <O> output type
   * @param <I> input type
   */
  protected interface ValueExtractor<O, I> {
    O extract(I input);
  }

  @Override
  public boolean add(Iterator<T> value) {
    while (value.hasNext()) {
      getPartition().add(getValueExtractor().extract(value.next()));
    }
    return true;
  }

  @Override
  public CollectionPartition<T1> get() {
    return getPartition();
  }
}
