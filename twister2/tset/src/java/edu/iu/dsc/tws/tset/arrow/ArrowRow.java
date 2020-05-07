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
package edu.iu.dsc.tws.tset.arrow;

import edu.iu.dsc.tws.api.tset.table.Row;

public class ArrowRow implements Row {
  private Object[] values;

  public static ArrowRow fromValues(Object... vals) {
    return new ArrowRow(vals);
  }

  public ArrowRow(Object[] vals) {
    this.values = vals;
  }

  @Override
  public int numberOfColumns() {
    return values.length;
  }

  @Override
  public Row duplicate() {
    return null;
  }

  @Override
  public Object get(int column) {
    return values[column];
  }

  @Override
  public String getString(int column) {
    return get(column).toString();
  }

  @Override
  public int getInt4(int column) {
    return (int) get(column);
  }

  @Override
  public double getFloat8(int column) {
    return (double) get(column);
  }

  @Override
  public float getFloat4(int column) {
    return (float) get(column);
  }

  @Override
  public long getInt8(int column) {
    return (long) get(column);
  }
}
