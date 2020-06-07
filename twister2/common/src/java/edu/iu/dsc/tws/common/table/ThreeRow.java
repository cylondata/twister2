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

public class ThreeRow implements Row {
  private Object val1;

  private Object val2;

  private Object val3;

  public ThreeRow(Object val1, Object val2, Object val3) {
    this.val1 = val1;
    this.val2 = val2;
    this.val3 = val3;
  }

  @Override
  public int numberOfColumns() {
    return 3;
  }

  @Override
  public Row duplicate() {
    return new ThreeRow(val1, val2, val3);
  }

  @Override
  public Object get(int column) {
    if (column == 0) {
      return val1;
    } else if (column == 1) {
      return val2;
    } else if (column == 2) {
      return val3;
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
    } else if (column == 2) {
      return (String) val3;
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
    } else if (column == 2) {
      return (int) val3;
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
    } else if (column == 2) {
      return (long) val3;
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
    } else if (column == 2) {
      return (double) val3;
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
    } else if (column == 2) {
      return (float) val3;
    } else {
      throw new Twister2RuntimeException("Invalid column index " + column + " only two columns");
    }
  }
}
