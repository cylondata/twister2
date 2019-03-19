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
package edu.iu.dsc.tws.api.tset.sources;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.tset.BaseSource;
import edu.iu.dsc.tws.dataset.DataObject;
import edu.iu.dsc.tws.dataset.DataPartition;

public class CacheSource<T> extends BaseSource<T> {
  private static final Logger LOG = Logger.getLogger(CacheSource.class.getName());

  //TODO: need to check this codes logic developed now just based on the data object API
  private int count = getDataObject().getPartitionCount();
  private int current = 0;
  private DataObject<T> datapoints = null;

  public CacheSource(DataObject<T> datapoints) {
    this.datapoints = datapoints;
  }

  @Override
  public boolean hasNext() {
    boolean hasNext = (current < count) ? getDataObject().getPartitions(current)
        .getConsumer().hasNext() : false;

    while (++current < count && !hasNext) {
      hasNext = getDataObject().getPartitions(current).getConsumer().hasNext();
    }
    return hasNext;
  }

  @Override
  public T next() {
    return getDataObject().getPartitions(current).getConsumer().next();
  }

  private List<T> getData() {
    if (datapoints == null) {
      LOG.fine("Data has not been added to the data object");
      return new ArrayList<>();
    }
    DataPartition<T>[] parts = datapoints.getPartitions();
    List<T> results = new ArrayList();
    for (DataPartition<T> part : parts) {
      results.add(part.getConsumer().next());
    }
    return results;
  }

  private DataObject<T> getDataObject() {
    return datapoints;
  }

  @Override
  public void prepare() {

  }
}
