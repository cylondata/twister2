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

import edu.iu.dsc.tws.api.dataset.DataPartitionConsumer;
import edu.iu.dsc.tws.api.tset.TSetContext;
import edu.iu.dsc.tws.api.tset.fn.BaseSourceFunc;
import edu.iu.dsc.tws.dataset.partition.DiskBackedCollectionPartition;

public class DiskPartitionBackedSource<T> extends BaseSourceFunc<T> {

  private DataPartitionConsumer<T> consumer;
  private String referencePrefix;

  public DiskPartitionBackedSource(String referencePrefix) {
    this.referencePrefix = referencePrefix;
  }

  @Override
  public boolean hasNext() {
    return this.consumer != null && this.consumer.hasNext();
  }

  @Override
  public T next() {
    return this.consumer.next();
  }

  @Override
  public void prepare(TSetContext ctx) {
    super.prepare(ctx);
//    String reference = this.referencePrefix + ctx.getIndex();
    String reference = ctx.getId() + ctx.getIndex();
    DiskBackedCollectionPartition<T> diskPartition = new DiskBackedCollectionPartition<>(
        0, ctx.getConfig(), reference);
    this.consumer = diskPartition.getConsumer();
  }
}
