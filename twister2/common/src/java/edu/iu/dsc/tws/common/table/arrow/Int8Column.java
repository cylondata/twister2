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
package edu.iu.dsc.tws.common.table.arrow;

import org.apache.arrow.vector.UInt8Vector;

public class Int8Column implements ArrowColumn<Long> {
  private UInt8Vector vector;

  private int currentIndex;

  public Int8Column(UInt8Vector intVector) {
    this.vector = intVector;
    this.currentIndex = 0;
  }

  @Override
  public void addValue(Long value) {
    vector.setSafe(currentIndex, value);
    currentIndex++;
  }

  @Override
  public Long get(int index) {
    return vector.get(index);
  }

  @Override
  public long currentSize() {
    return vector.getBufferSize();
  }
}
