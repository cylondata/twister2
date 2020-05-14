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
package edu.iu.dsc.tws.api.tset.table;

public interface TableBuilder {
  /**
   * Add a row to build the tbale
   * @param row row
   */
  void add(Row row);

  /**
   * Build the table at the end
   * @return the built table
   */
  TSetTable build();

  /**
   * Get the current size of the table
   */
  long currentSize();
}
