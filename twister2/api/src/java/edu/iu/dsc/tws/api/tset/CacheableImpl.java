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
package edu.iu.dsc.tws.api.tset;

import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import edu.iu.dsc.tws.dataset.DataObject;
import edu.iu.dsc.tws.dataset.DataPartition;

public class CacheableImpl<T> implements Cacheable<T> {
  private static final Logger LOG = Logger.getLogger(CacheableImpl.class.getName());
//TODO: need to define the APO for cachble properly
  private DataObject<T> data = null;

  public CacheableImpl(DataObject<T> data) {
    this.data = data;
  }

  @Override
  public List<T> getData() {
    if (data == null) {
      LOG.fine("Data has not been added to the data object");
      return new ArrayList<>();
    }
    DataPartition<T>[] parts = data.getPartitions();
    List<T> results = new ArrayList();
    for (DataPartition<T> part : parts) {
      results.add(part.getConsumer().next());
    }
    return results;
  }

  @Override
  public DataObject<T> getDataObject() {
    return data;
  }

  @Override
  public T getPartitionData(int partitionId) {
    return data.getPartitions(partitionId).getConsumer().next();
  }

  @Override
  public boolean addData(T value) {
    throw new UnsupportedOperationException("Not Supported yet");
  }
}
