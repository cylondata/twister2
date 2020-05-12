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
package edu.iu.dsc.tws.tset.fn.row;

import java.util.Iterator;

import edu.iu.dsc.tws.api.tset.TSetContext;
import edu.iu.dsc.tws.api.tset.fn.ComputeCollectorFunc;
import edu.iu.dsc.tws.api.tset.fn.MapFunc;
import edu.iu.dsc.tws.api.tset.fn.RecordCollector;
import edu.iu.dsc.tws.api.tset.table.Row;

public class RowMapCompute implements ComputeCollectorFunc<Row, Iterator<Row>> {
  private MapFunc<Row, Row> mapFn;

  public RowMapCompute(MapFunc<Row, Row> mapFn) {
    this.mapFn = mapFn;
  }

  @Override
  public void compute(Iterator<Row> input, RecordCollector<Row> output) {
    while (input.hasNext()) {
      Row next = input.next();
      Row out = mapFn.map(next);
      output.collect(out);
    }
  }

  @Override
  public void prepare(TSetContext context) {
    mapFn.prepare(context);
  }

  @Override
  public void close() {
    mapFn.close();
  }
}
