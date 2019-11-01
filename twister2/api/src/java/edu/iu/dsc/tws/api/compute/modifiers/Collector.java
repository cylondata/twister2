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

package edu.iu.dsc.tws.api.compute.modifiers;

import edu.iu.dsc.tws.api.dataset.DataPartition;

/**
 * Special task for collecting the output from tasks
 */
public interface Collector {

  /**
   * get the collected valued
   *
   * @return get the default output
   * @deprecated use the data partition
   */
  @Deprecated
  default DataPartition<?> get() {
    return null;
  }

  /**
   * get the collected valued
   *
   * @param name name of the value to collect
   * @return the partition of the data
   */
  default DataPartition<?> get(String name) {
    return null;
  }

  /**
   * This method should return a set of collectible names, that can be collected from this
   * collector. These names will be used by task plan builder to cross validate parallelism
   * between two task graphs.
   * <p>
   * If {@link Collector} C of task graph TG1 collects variable "var1" and C's parallelism is n,  If
   * {@link Receptor} R of task graph TG2 is interested in receiving "var1", R's parallelism should
   * be equal to n and, R should have the same distribution as C among the workers.
   *
   * @return set of names of collectbles
   */

  default IONames getCollectibleNames() {
    return IONames.declare();
  }
}
