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
package edu.iu.dsc.tws.column;

import org.apache.arrow.vector.FieldVector;

import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;

public class ArrowTable {
  private final long rows;

  private FieldVector[] columns;

  public ArrowTable(FieldVector... columns) {
    this.columns = columns;
    this.rows = columns.length > 0 ? columns[0].getValueCount() + columns[0].getNullCount() : 0;

    for (FieldVector v : columns) {
      if (v.getNullCount() + v.getValueCount() != rows) {
        throw new Twister2RuntimeException("The rows needs to be equal");
      }
    }
  }

  public long getRows() {
    return rows;
  }

  public FieldVector[] getColumns() {
    return columns;
  }

  public int getNumberOfColumns() {
    return columns.length;
  }
}
