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

import edu.iu.dsc.tws.api.tset.TSetContext;
import edu.iu.dsc.tws.api.tset.fn.BaseSinkFunc;
import edu.iu.dsc.tws.dataset.partition.DiskBackedCollectionPartition;
import edu.iu.dsc.tws.tset.TSetUtils;

public class DiskPersistSingleSink<T> extends BaseSinkFunc<T> {
  private DiskBackedCollectionPartition<T> partition;

  private final String referencePrefix;

  /**
   * Creates an instance of {@link DiskPersistSingleSink} with a referencePrefix
   *
   * @param referencePrefix referencePrefix will be used to uniquely identify the set of
   * disk partitions created with this function
   */
  public DiskPersistSingleSink(String referencePrefix) {
    this.referencePrefix = referencePrefix;
  }

  @Override
  public void prepare(TSetContext ctx) {
    super.prepare(ctx);
    String reference = TSetUtils.getDiskCollectionReference(this.referencePrefix, ctx);
    this.partition = new DiskBackedCollectionPartition<>(0, ctx.getConfig(),
        reference);
  }

  @Override
  public boolean add(T value) {
    this.partition.add(value);
    return true;
  }

  @Override
  public void close() {
    partition.close();
  }

  @Override
  public DiskBackedCollectionPartition<T> get() {
    return partition;
  }
}
