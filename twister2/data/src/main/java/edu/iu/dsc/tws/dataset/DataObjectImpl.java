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
package edu.iu.dsc.tws.dataset;

import java.util.HashMap;
import java.util.Map;

import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.dataset.DataObject;
import edu.iu.dsc.tws.api.dataset.DataPartition;

public class DataObjectImpl<T> implements DataObject<T> {
  private Map<Integer, DataPartition<T>> partitions = new HashMap<>();

  private Config config;

  private String id;

  public DataObjectImpl(Config conf) {
    this.config = conf;
  }

  public DataObjectImpl(String name, Config conf) {
    this.id = name;
    this.config = conf;
  }

  @Override
  public void addPartition(DataPartition<T> partition) {
    partitions.put(partition.getPartitionId(), partition);
  }

  public DataPartition<T>[] getPartition() {
    DataPartition<T>[] parts = new DataPartition[partitions.size()];
    return partitions.values().toArray(parts);
  }

  @Override
  public DataPartition<T> getPartition(int partitionId) {
    return partitions.get(partitionId);
  }

  @Override
  public int getPartitionCount() {
    return partitions.size();
  }

  @Override
  public String getID() {
    if (id != null && !id.isEmpty()) {
      return id;
    } else {
      throw new RuntimeException("Unnamed data object");
    }
  }
}
