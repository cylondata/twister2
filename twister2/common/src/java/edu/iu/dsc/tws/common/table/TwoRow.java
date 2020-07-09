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
  private Object[] vals;


  public TwoRow(Object val1, Object val2) {
    this.vals = new Object[]{val1, val2};
  }

  @Override
  public int numberOfColumns() {
    return 2;
  }

  @Override
  public Row duplicate() {
    return new TwoRow(vals[0], vals[1]);
  }

  @Override
  public Object get(int column) {
    if (column <= 1) {
      return vals[column];
    } else {
      throw new Twister2RuntimeException("Invalid column index " + column + " only two columns");
    }
  }

  @Override
  public String getString(int column) {
    if (column <= 1) {
      return (String) vals[column];
    } else {
      throw new Twister2RuntimeException("Invalid column index " + column + " only two columns");
    }
  }

  @Override
  public int getInt4(int column) {
    if (column <= 1) {
      return (int) vals[column];
    } else {
      throw new Twister2RuntimeException("Invalid column index " + column + " only two columns");
    }
  }

  @Override
  public long getInt8(int column) {
    if (column <= 1) {
      return (long) vals[column];
    } else {
      throw new Twister2RuntimeException("Invalid column index " + column + " only two columns");
    }
  }

  @Override
  public double getFloat8(int column) {
    if (column <= 1) {
      return (double) vals[column];
    } else {
      throw new Twister2RuntimeException("Invalid column index " + column + " only two columns");
    }
  }

  @Override
  public float getFloat4(int column) {
    if (column <= 1) {
      return (float) vals[column];
    } else {
      throw new Twister2RuntimeException("Invalid column index " + column + " only two columns");
    }
  }

  @Override
  public short getInt2(int column) {
    if (column <= 1) {
      return (short) vals[column];
    } else {
      throw new Twister2RuntimeException("Invalid column index " + column + " only two columns");
    }
  }

  @Override
  public byte[] getByte(int column) {
    if (column <= 1) {
      return (byte[]) vals[column];
    } else {
      throw new Twister2RuntimeException("Invalid column index " + column + " only two columns");
    }
  }
}
