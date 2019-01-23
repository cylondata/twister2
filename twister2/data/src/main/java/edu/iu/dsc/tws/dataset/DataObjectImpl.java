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

import edu.iu.dsc.tws.common.config.Config;

public class DataObjectImpl<T> implements DataObject<T> {
  private Map<Integer, DataPartition<T>> partitions = new HashMap<>();

  public DataObjectImpl(Config config) {
  }

  @Override
  public void addPartition(DataPartition<T> partition) {
    partitions.put(partition.getPartitionId(), partition);
  }

  public DataPartition<T>[] getPartitions() {
    DataPartition<T>[] parts = new DataPartition[partitions.values().size()];
    int i = 0;
    for (DataPartition<T> t : partitions.values()) {
      parts[i++] = t;
    }
    return parts;
  }

  @Override
  public DataPartition<T> getPartitions(int partitionId) {
    return partitions.get(partitionId);
  }
}
