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

/**
 * Partition of a distributed set
 *
 * @param <T> partition
 */
public interface PSet<T> {
  /**
   * Get the process id this partition belongs to
   *
   * @return the process id
   */
  int getProcId();

  /**
   * Get the id of the partition
   * @return the id of the partition
   */
  int partitionId();

  /**
   * Weather there is a next record
   *
   * @return true if there is a next record
   */
  boolean hasNext();

  /**
   * Get the next record
   *
   * @return the next
   */
  T next();
}
