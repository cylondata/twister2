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
package edu.iu.dsc.tws.task.api;

import java.io.Serializable;
import java.util.Set;

/**
 * Task partitioner
 */
public interface TaskPartitioner extends Serializable {
  /**
   * Prepare the partition
   * @param context
   * @param sources
   * @param destinations
   */
  void prepare(TaskContext context, Set<Integer> sources, Set<Integer> destinations);

  /**
   * Get a partition
   * @param source
   * @param data
   * @return
   */
  int partition(int source, Object data);

  /**
   * Indicate that we are using this partition
   * @param source
   * @param partition
   */
  void commit(int source, int partition);
}
