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

import java.util.Iterator;
import java.util.List;

import edu.iu.dsc.tws.api.tset.table.Row;
import edu.iu.dsc.tws.api.tset.table.TSetTable;
import edu.iu.dsc.tws.common.table.Table;
import edu.iu.dsc.tws.common.table.ArrowColumn;

public class TSetTableImpl implements TSetTable {
  private Table table;

  public TSetTableImpl(Table table) {
    this.table = table;
  }

  @Override
  public Table table() {
    return table;
  }

  @Override
  public Iterator<Row> getRowIterator() {
    return new RowIterator();
  }

  private class RowIterator implements Iterator<Row> {
    private int index = 0;

    @Override
    public boolean hasNext() {
      return index < table.rowCount();
    }

    @Override
    public Row next() {
      List<ArrowColumn> columns = table.getColumns();
      Object[] vals = new Object[columns.size()];
      Row row = new ArrowRow(vals);
      for (int i = 0; i < columns.size(); i++) {
        ArrowColumn c = columns.get(i);
        vals[i] = c.get(index);
      }
      index++;
      return row;
    }
  }
}
