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
package edu.iu.dsc.tws.tset.table;

import java.util.ArrayList;
import java.util.List;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarCharVector;

import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;
import edu.iu.dsc.tws.api.tset.table.Field;
import edu.iu.dsc.tws.api.tset.table.Row;
import edu.iu.dsc.tws.api.tset.table.RowSchema;
import edu.iu.dsc.tws.api.tset.table.TSetTable;
import edu.iu.dsc.tws.api.tset.table.TableBuilder;
import edu.iu.dsc.tws.common.table.arrow.ArrowColumn;
import edu.iu.dsc.tws.common.table.arrow.Float4Column;
import edu.iu.dsc.tws.common.table.arrow.Float8Column;
import edu.iu.dsc.tws.common.table.arrow.Int4Column;
import edu.iu.dsc.tws.common.table.arrow.Int8Column;
import edu.iu.dsc.tws.common.table.arrow.StringColumn;

public class ArrowTableBuilder implements TableBuilder {

  private List<ArrowColumn> columns = new ArrayList<>();

  private long currentSize = 0;

  public ArrowTableBuilder(RowSchema schema, BufferAllocator allocator) {
    for (Field t : schema.getFields()) {
      if (t.getType().equals(MessageTypes.INTEGER)) {
        IntVector vector = new IntVector(t.getName(), allocator);
        columns.add(new Int4Column(vector));
      } else if (t.getType().equals(MessageTypes.FLOAT)) {
        Float4Vector vector = new Float4Vector(t.getName(), allocator);
        columns.add(new Float4Column(vector));
      } else if (t.getType().equals(MessageTypes.DOUBLE)) {
        Float8Vector vector = new Float8Vector(t.getName(), allocator);
        columns.add(new Float8Column(vector));
      } else if (t.getType().equals(MessageTypes.LONG)) {
        UInt8Vector vector = new UInt8Vector(t.getName(), allocator);
        columns.add(new Int8Column(vector));
      } else if (t.getType().equals(MessageTypes.STRING)) {
        VarCharVector vector = new VarCharVector(t.getName(), allocator);
        columns.add(new StringColumn(vector));
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
  public TSetTable build() {
    return new TSetTableImpl(columns);
  }

  @Override
  public long currentSize() {
    return currentSize;
  }
}
