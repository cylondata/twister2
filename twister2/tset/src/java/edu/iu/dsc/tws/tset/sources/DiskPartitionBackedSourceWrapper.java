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
package edu.iu.dsc.tws.tset.sources;

import edu.iu.dsc.tws.api.dataset.DataPartition;
import edu.iu.dsc.tws.api.tset.TSetContext;
import edu.iu.dsc.tws.api.tset.fn.BaseSourceFunc;

/**
 * Wraps the data partition created by underlying {@link DiskPartitionBackedSource}. This is used
 * for checkpointing purposes. Not intended to be used as a usual source.
 *
 * @param <T> type
 */
public class DiskPartitionBackedSourceWrapper<T> extends BaseSourceFunc<T> {
  private DiskPartitionBackedSource<T> sourceFunc;

  public DiskPartitionBackedSourceWrapper(DiskPartitionBackedSource<T> sourceFunc) {
    this.sourceFunc = sourceFunc;
  }

  @Override
  public void prepare(TSetContext ctx) {
    super.prepare(ctx);
    // prepare the underlying sourceFunc. So that it would be able to properly populate the
    // underlying data partition
    this.sourceFunc.prepare(ctx);
  }

  @Override
  public boolean hasNext() {
    // nothing to return!
    return false;
  }

  @Override
  public T next() {
    // nothing to return!
    return null;
  }

  public DataPartition<?> get() {
    return sourceFunc.get();
  }
}
