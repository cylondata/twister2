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

package edu.iu.dsc.tws.api.dataset;

import java.io.Serializable;

/**
 * Distributed data set
 *
 * @param <T> the distributed set interface
 */
public interface DataObject<T> extends Serializable {

  /**
   * Add a partition to the data object
   *
   * @param partition the partition
   */
  void addPartition(DataPartition<T> partition);

  /**
   * Get the list of partitions for a process
   *
   * @return the partitions
   */
  DataPartition<T>[] getPartition();

  /**
   * Get the partition with the specific partition id
   *
   * @param partitionId partition id
   * @return DataPartition
   */
  DataPartition<T> getPartition(int partitionId);

  /**
   * Get the number of partitions currently in the DataObject
   *
   * @return the number of partitions
   */
  int getPartitionCount();

  /**
   * ID of the data object. The ID needs to be deterministic
   * @return ID
   */
  String getID();
}
