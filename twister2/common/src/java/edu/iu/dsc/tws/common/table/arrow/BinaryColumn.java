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

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VarBinaryVector;

import edu.iu.dsc.tws.common.table.ArrowColumn;

public class BinaryColumn implements ArrowColumn<byte[]> {
  private VarBinaryVector vector;
  private int currentIndex;

  public BinaryColumn(VarBinaryVector vector) {
    this.vector = vector;
  }

  @Override
  public void addValue(byte[] value) {
    vector.setSafe(currentIndex, value);
    currentIndex++;
    vector.setValueCount(currentIndex);
  }

  @Override
  public byte[] get(int index) {
    return vector.get(index);
  }

  @Override
  public FieldVector getVector() {
    return vector;
  }

  @Override
  public long currentSize() {
    return vector.getBufferSize();
  }
}
