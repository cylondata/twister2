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
package edu.iu.dsc.tws.api.task;

import edu.iu.dsc.tws.dataset.DataSet;

/**
 * Add input to a task graph
 */
public interface Receptor {
  /**
   * This method is called when the data is available
   * @param name name of the input
   * @param data input data
   */
  void add(String name, DataSet<Object> data);
}
