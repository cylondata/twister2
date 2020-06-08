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
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.tset.TSetContext;
import edu.iu.dsc.tws.api.tset.fn.ApplyFunc;
import edu.iu.dsc.tws.api.tset.fn.ComputeCollectorFunc;
import edu.iu.dsc.tws.api.tset.fn.RecordCollector;
import edu.iu.dsc.tws.common.table.Row;

public class RowForEachCompute implements ComputeCollectorFunc<Row, Iterator<Row>> {
  private static final Logger LOG = Logger.getLogger(RowForEachCompute.class.getName());

  private ApplyFunc<Row> applyFn;

  private TSetContext ctx;

  public RowForEachCompute(ApplyFunc<Row> applyFunction) {
    this.applyFn = applyFunction;
  }

  @Override
  public void prepare(TSetContext context) {
    ctx = context;
    applyFn.prepare(context);
  }

  @Override
  public void close() {
    applyFn.close();
  }

  @Override
  public void compute(Iterator<Row> input, RecordCollector<Row> output) {
    try {
      while (input.hasNext()) {
        applyFn.apply(input.next());
      }
    } catch (IndexOutOfBoundsException e) {
      LOG.info("Apply function index " + ctx.getIndex() + " id " + ctx.getWorkerId());
      throw e;
    }
  }
}
