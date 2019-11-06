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
import edu.iu.dsc.tws.api.dataset.DataPartitionConsumer;
import edu.iu.dsc.tws.api.tset.TSetContext;
import edu.iu.dsc.tws.api.tset.fn.BaseSourceFunc;

public class DataPartitionSourceFunc<T> extends BaseSourceFunc<T> {
//  private static final Logger LOG = Logger.getLogger(CacheSource.class.getName());

  private String storedKey;
  private transient DataPartitionConsumer<T> currentConsumer;

  public DataPartitionSourceFunc(String storedDataKey) {
    this.storedKey = storedDataKey;
  }

  @Override
  public boolean hasNext() {
    return currentConsumer.hasNext();
  }

  @Override
  public T next() {
    return currentConsumer.next();
  }

  @Override
  public void prepare(TSetContext ctx) {
    super.prepare(ctx);
    // retrieve the partition from the context
    DataPartition<T> data = (DataPartition<T>) getInput(storedKey);
    this.currentConsumer = data.getConsumer();
  }
}
