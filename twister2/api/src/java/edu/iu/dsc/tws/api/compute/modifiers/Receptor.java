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

import java.util.Collections;
import java.util.Set;

import edu.iu.dsc.tws.api.dataset.DataObject;
import edu.iu.dsc.tws.api.dataset.DataPartition;

/**
 * Add input to a task graph
 */
public interface Receptor {
  /**
   * This method is called when the data is available
   *
   * @param name name of the input
   * @param data input data
   * @deprecated Use {@link Receptor#add(String, DataPartition)} instead
   */
  @Deprecated
  void add(String name, DataObject<?> data);


  default void add(String name, DataPartition<?> data) {
    //todo remove default
  }

  /**
   * This method should return a set of receivable names, that are expected by this receptor.
   * These names will be used by task plan builder to cross validate parallelism
   * between two task graphs.
   * <p>
   * If {@link Collector} C of task graph TG1 collects variable "var1" and C's parallelism is n, If
   * {@link Receptor} R of task graph TG2 is interested in receiving "var1", R's parallelism should
   * be equal to n and, R should have the same distribution as C among the workers.
   *
   * @return get the set of names
   */
  default Set<String> getReceivableNames() {
    return Collections.emptySet();
  }
}
