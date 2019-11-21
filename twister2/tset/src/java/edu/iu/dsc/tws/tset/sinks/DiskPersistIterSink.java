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
import edu.iu.dsc.tws.dataset.partition.DiskBackedCollectionPartition;

/**
 * A disk based persisted TSet
 *
 * @param <T> TSet data type
 */
public class DiskPersistIterSink<T> extends StoreIterSink<T, T> {
  private DiskBackedCollectionPartition<T> partition;

  private String referencePrefix;

  /**
   * Creates an instance of {@link DiskPersistIterSink} with a referencePrefix
   *
   * @param referencePrefix referencePrefix will be used to uniquely identify the set of
   * disk partitions created with this function
   */
  public DiskPersistIterSink(String referencePrefix) {
    this.referencePrefix = referencePrefix;
  }

  @Override
  public void prepare(TSetContext ctx) {
    super.prepare(ctx);

//    String reference = referencePrefix + ctx.getIndex();
    String reference = ctx.getId() + ctx.getIndex();
    // buffered partition with 0 frames in memory. Then everything will be written to the memory
    this.partition = new DiskBackedCollectionPartition<>(0,
        ctx.getConfig(), reference);
  }

  @Override
  public void close() {
    // Explicitly closing the @DiskBackedCollectionPartition so that it would flush the remaining
    // data to disk
    this.partition.close();
  }

  @Override
  protected DiskBackedCollectionPartition<T> getPartition() {
    return this.partition;
  }

  @Override
  protected ValueExtractor<T, T> getValueExtractor() {
    return input -> input;
  }
}
