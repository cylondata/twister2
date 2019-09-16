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
package edu.iu.dsc.tws.api.compute;

import java.io.Serializable;
import java.util.Set;

/**
 * Partition the value into a destination.
 */
public interface TaskPartitioner<T> extends Serializable {
  /**
   * Prepare the partition with sources and destinations. The sources have values to partition
   * and the partitions should be among destinations
   */
  void prepare(Set<Integer> sources, Set<Integer> destinations);

  /**
   * Get a partition id, it should be from the set of <code>destinations</code>
   *
   * @param source source of the data
   * @param data  data to be partitioned
   * @return partition id
   */
  int partition(int source, T data);

  /**
   * Indicate that we are using this partition
   *
   * @param source source of the partition
   * @param partition partition to commit
   */
  void commit(int source, int partition);
}
