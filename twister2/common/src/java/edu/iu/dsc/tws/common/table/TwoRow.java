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
package edu.iu.dsc.tws.common.table;

import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;

public class TwoRow implements Row {
  private Object val1;

  private Object val2;

  public TwoRow(Object val1, Object val2) {
    this.val1 = val1;
    this.val2 = val2;
  }

  @Override
  public int numberOfColumns() {
    return 2;
  }

  @Override
  public Row duplicate() {
    return new TwoRow(val1, val2);
  }

  @Override
  public Object get(int column) {
    if (column == 0) {
      return val1;
    } else if (column == 1) {
      return val2;
    } else {
      throw new Twister2RuntimeException("Invalid column index " + column + " only two columns");
    }
  }

  @Override
  public String getString(int column) {
    if (column == 0) {
      return (String) val1;
    } else if (column == 1) {
      return (String) val2;
    } else {
      throw new Twister2RuntimeException("Invalid column index " + column + " only two columns");
    }
  }

  @Override
  public int getInt4(int column) {
    if (column == 0) {
      return (int) val1;
    } else if (column == 1) {
      return (int) val2;
    } else {
      throw new Twister2RuntimeException("Invalid column index " + column + " only two columns");
    }
  }

  @Override
  public long getInt8(int column) {
    if (column == 0) {
      return (long) val1;
    } else if (column == 1) {
      return (long) val2;
    } else {
      throw new Twister2RuntimeException("Invalid column index " + column + " only two columns");
    }
  }

  @Override
  public double getFloat8(int column) {
    if (column == 0) {
      return (double) val1;
    } else if (column == 1) {
      return (double) val2;
    } else {
      throw new Twister2RuntimeException("Invalid column index " + column + " only two columns");
    }
  }

  @Override
  public float getFloat4(int column) {
    if (column == 0) {
      return (float) val1;
    } else if (column == 1) {
      return (float) val2;
    } else {
      throw new Twister2RuntimeException("Invalid column index " + column + " only two columns");
    }
  }
}
