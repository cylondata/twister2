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

import com.google.common.base.Preconditions;

public class OneRow implements Row {
  private Object val;

  public OneRow(Object val) {
    this.val = val;
  }

  @Override
  public int numberOfColumns() {
    return 1;
  }

  @Override
  public Row duplicate() {
    return null;
  }

  private void check(int column) {
    Preconditions.checkArgument(column == 0, "Only a single column");
  }

  @Override
  public Object get(int column) {
    check(column);
    return val;
  }

  @Override
  public String getString(int column) {
    check(column);
    return val.toString();
  }

  @Override
  public int getInt4(int column) {
    return (int) val;
  }

  @Override
  public long getInt8(int column) {
    return (long) val;
  }

  @Override
  public double getFloat8(int column) {
    return (double) val;
  }

  @Override
  public float getFloat4(int column) {
    return (float) val;
  }
}
