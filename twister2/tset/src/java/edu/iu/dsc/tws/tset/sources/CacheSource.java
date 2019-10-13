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

import edu.iu.dsc.tws.api.dataset.DataObject;
import edu.iu.dsc.tws.api.dataset.DataPartitionConsumer;
import edu.iu.dsc.tws.api.tset.TSetContext;
import edu.iu.dsc.tws.api.tset.fn.BaseSourceFunc;

public class CacheSource<T> extends BaseSourceFunc<T> {
//  private static final Logger LOG = Logger.getLogger(CacheSource.class.getName());

  private int end;
  private int current;
  private DataObject<T> data;
  private transient DataPartitionConsumer<T> currentConsumer;

  public CacheSource(DataObject<T> datapoints) {
    this.data = datapoints;
    this.end = datapoints.getPartitionCount();
    this.current = 0;
  }

  @Override
  public boolean hasNext() {
    return currentConsumer.hasNext();
  }

  @Override
  public T next() {
    T ret = currentConsumer.next();
    updateConsumer();
    return ret;
  }

  //  check if the consumer has reached the end. if its at the end, attach the next consumer.
  private void updateConsumer() {
    if (!currentConsumer.hasNext() && (++current < end)) {
      currentConsumer = data.getPartitions()[current].getConsumer();
    }
  }

  @Override
  public void prepare(TSetContext ctx) {
    super.prepare(ctx);

    // set the current consumer to the partition at idx 0 of the partition array
    this.currentConsumer = data.getPartitions()[0].getConsumer();
  }
}
