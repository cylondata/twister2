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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import edu.iu.dsc.tws.api.dataset.DataObject;
import edu.iu.dsc.tws.api.dataset.DataPartition;

/**
 * All Tsets that are cachable need to implement this interface
 * This interface defines the methods that other classes can use to
 * access the cached data
 */
public interface Storable<T> extends TBase, Serializable {

  /**
   * retrieve data saved in the TSet
   * <p>
   * NOTE: use this method only when you need to pull the data from the data object. Otherwise
   * this would unnecessarily loads data to the memory from the partition consumer
   *
   * @return dataObject
   */
  default List<T> getData() {
    List<T> results = new ArrayList<>();

    if (getDataObject() != null) {
      for (DataPartition<T> partition : getDataObject().getPartitions()) {
        while (partition.getConsumer().hasNext()) {
          results.add(partition.getConsumer().next());
        }
      }
    }

    return results;
  }

  /**
   * get the data from the given partition.
   * <p>
   * NOTE: use this method only when you need to pull the data from the data object. Otherwise
   * this would unnecessarily loads data to the memory from the partition consumer
   *
   * @param partitionId the partition ID
   * @return the data related to the given partition
   */
  default List<T> getData(int partitionId) {
    DataPartition<T> partition = getDataObject().getPartition(partitionId);
    List<T> results = new ArrayList<>();
    while (partition.getConsumer().hasNext()) {
      results.add(partition.getConsumer().next());
    }

    return results;
  }

  /**
   * retrieve data saved in the TSet
   *
   * @return dataObject
   */
  DataObject<T> getDataObject();

//  /**
//   * Add Data to the data object
//   *
//   * @param data value to be added
//   * @return true if the data was added successfully or false otherwise
//   */
//  boolean updateDataObject(DataObject<T> data);

}
