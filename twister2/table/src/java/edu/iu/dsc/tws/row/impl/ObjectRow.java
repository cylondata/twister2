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
package edu.iu.dsc.tws.row.impl;

import edu.iu.dsc.tws.row.Row;

/**
 * Keep the attributes as objects
 */
public class ObjectRow implements Row {
  /**
   * The column values as an array
   */
  private Object[] columnValues;

  public ObjectRow(Object[] columnValues) {
    this.columnValues = columnValues;
  }

  @Override
  public int getInt32Value(int index) {
    return (int) columnValues[index];
  }

  @Override
  public int getInt32Value(String name) {
    // get the index of the column name
    return 0;
  }
}
