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

import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.tset.TSetContext;
import edu.iu.dsc.tws.dataset.partition.DiskBackedCollectionPartition;

public class DiskPersistGatherSink<T> extends StoreIterSink<Tuple<Integer, T>, T> {

  private DiskBackedCollectionPartition<T> partition;

  @Override
  public void prepare(TSetContext ctx) {
    super.prepare(ctx);
    this.partition = new DiskBackedCollectionPartition<>(0, ctx.getConfig());
  }

  @Override
  protected DiskBackedCollectionPartition<T> getPartition() {
    return partition;
  }

  @Override
  protected ValueExtractor<T, Tuple<Integer, T>> getValueExtractor() {
    return Tuple::getValue;
  }

  @Override
  public void close() {
    this.partition.close();
  }
}
