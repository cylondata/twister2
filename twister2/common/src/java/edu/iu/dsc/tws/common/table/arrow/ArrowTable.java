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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.logging.Logger;

import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.Float4Vector;
import org.apache.arrow.vector.Float8Vector;
import org.apache.arrow.vector.IntVector;
import org.apache.arrow.vector.UInt8Vector;
import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.types.pojo.Schema;

import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;
import edu.iu.dsc.tws.common.table.ArrowColumn;
import edu.iu.dsc.tws.common.table.ArrowRow;
import edu.iu.dsc.tws.common.table.Row;
import edu.iu.dsc.tws.common.table.Table;

public class ArrowTable implements Table {
  private static final Logger LOG = Logger.getLogger(ArrowTable.class.getName());

  private List<ArrowColumn> columns;

  private Schema schema;

  private int rows;

  public ArrowTable(int rows, List<ArrowColumn> columns) {
    this(null, rows, columns);
  }

  public ArrowTable(Schema schema, int rows, List<ArrowColumn> columns) {
    this.columns = columns;
    this.schema = schema;
    this.rows = (int) columns.get(0).getVector().getValueCount();
  }

  public ArrowTable(Schema schema, List<FieldVector> vectors) {
    columns = new ArrayList<>();
    for (FieldVector vector : vectors) {
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

  public Schema getSchema() {
    return schema;
  }

  @Override
  public int rowCount() {
    return rows;
  }

  public List<ArrowColumn> getColumns() {
    return columns;
  }

  @Override
  public Iterator<Row> getRowIterator() {
    return new RowIterator();
  }

  private class RowIterator implements Iterator<Row> {
    private int index = 0;

    @Override
    public boolean hasNext() {
      return index < rowCount();
    }

    @Override
    public Row next() {
      try {
        List<ArrowColumn> cols = getColumns();
        Object[] vals = new Object[cols.size()];
        Row row = new ArrowRow(vals);
        for (int i = 0; i < cols.size(); i++) {
          ArrowColumn c = cols.get(i);
          vals[i] = c.get(index);
        }
        index++;
        return row;
      } catch (IndexOutOfBoundsException e) {
        LOG.severe("Index out of bounds " + index + " row count " + rowCount());
        throw e;
      }
    }
  }
}
