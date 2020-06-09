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

import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.Schema;

import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;
import edu.iu.dsc.tws.common.table.arrow.ArrowTable;
import edu.iu.dsc.tws.common.table.arrow.BinaryColumn;
import edu.iu.dsc.tws.common.table.arrow.Float4Column;
import edu.iu.dsc.tws.common.table.arrow.Float8Column;
import edu.iu.dsc.tws.common.table.arrow.Int4Column;
import edu.iu.dsc.tws.common.table.arrow.Int8Column;
import edu.iu.dsc.tws.common.table.arrow.StringColumn;

public class ArrowTableBuilder implements TableBuilder {

  private List<ArrowColumn> columns = new ArrayList<>();

  private long currentSize = 0;

  public ArrowTableBuilder(Schema schema, BufferAllocator allocator) {
    for (Field t : schema.getFields()) {
      FieldVector vector = t.createVector(allocator);
      if (vector instanceof IntVector) {
        columns.add(new Int4Column((IntVector) vector));
      } else if (vector instanceof Float4Vector) {
        columns.add(new Float4Column((Float4Vector) vector));
      } else if (vector instanceof Float8Vector) {
        columns.add(new Float8Column((Float8Vector) vector));
      } else if (vector instanceof UInt8Vector) {
        columns.add(new Int8Column((UInt8Vector) vector));
      } else if (vector instanceof VarCharVector) {
        columns.add(new StringColumn((VarCharVector) vector));
      } else if (vector instanceof VarBinaryVector) {
        columns.add(new BinaryColumn((VarBinaryVector) vector));
      } else {
        throw new Twister2RuntimeException("Un-recognized message type");
      }
    }
  }

  @Override
  public void add(Row row) {
    currentSize = 0;
    for (int i = 0; i < columns.size(); i++) {
      ArrowColumn ac = columns.get(i);
      ac.addValue(row.get(i));
      currentSize += ac.currentSize();
    }
  }

  @Override
  public List<ArrowColumn> getColumns() {
    return columns;
  }

  @Override
  public Table build() {
    return new ArrowTable((int) columns.get(0).currentSize(), columns);
  }

  @Override
  public long currentSize() {
    return currentSize;
  }
}
