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
package edu.iu.dsc.tws.tset.ops.row;

import java.util.Iterator;
import java.util.Map;

import edu.iu.dsc.tws.api.compute.IMessage;
import edu.iu.dsc.tws.api.compute.TaskContext;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;
import edu.iu.dsc.tws.api.resource.WorkerEnvironment;
import edu.iu.dsc.tws.api.tset.TSetConstants;
import edu.iu.dsc.tws.api.tset.fn.ComputeCollectorFunc;
import edu.iu.dsc.tws.api.tset.fn.RecordCollector;
import edu.iu.dsc.tws.api.tset.fn.TFunction;
import edu.iu.dsc.tws.api.tset.schema.RowSchema;
import edu.iu.dsc.tws.common.table.ArrowTableBuilder;
import edu.iu.dsc.tws.common.table.Row;
import edu.iu.dsc.tws.common.table.Table;
import edu.iu.dsc.tws.common.table.TableBuilder;
import edu.iu.dsc.tws.common.table.arrow.TableRuntime;
import edu.iu.dsc.tws.tset.ops.BaseComputeOp;
import edu.iu.dsc.tws.tset.sets.BaseTSet;

public class RowComupteCollectorOp extends BaseComputeOp<Table> {

  private ComputeCollectorFunc<Row, Iterator<Row>> computeFunction;

  private TableBuilder builder;

  /**
   * Table max size set to 64MB
   */
  private long tableMaxSize = 64000000;

  /**
   * Table runtime to use, it contains
   */
  private TableRuntime runtime;

  /**
   * The output schema
   */
  private RowSchema schema;

  public RowComupteCollectorOp() {
  }

  public RowComupteCollectorOp(ComputeCollectorFunc<Row, Iterator<Row>> computeFunction,
                               BaseTSet origin, Map<String, String> receivables) {
    super(origin, receivables);
    this.computeFunction = computeFunction;
  }

  @Override
  public void prepare(Config cfg, TaskContext ctx) {
    super.prepare(cfg, ctx);
    runtime = WorkerEnvironment.getSharedValue(TableRuntime.TABLE_RUNTIME_CONF,
        TableRuntime.class);
    if (runtime == null) {
      throw new Twister2RuntimeException("Table runtime must be set");
    }

    schema = (RowSchema) ctx.getConfig(TSetConstants.OUTPUT_SCHEMA_KEY);
    tableMaxSize = cfg.getLongValue("twister2.table.max.size", tableMaxSize);
    builder = new ArrowTableBuilder(schema.toArrowSchema(), runtime.getRootAllocator());
  }

  @Override
  public boolean execute(IMessage<Table> content) {
    CollectorImp collectorImp = new CollectorImp();
    Table table = content.getContent();
    computeFunction.compute(table.getRowIterator(), collectorImp);
    collectorImp.close();
    writeEndToEdges();
    computeFunction.close();
    return true;
  }

  private class CollectorImp implements RecordCollector<Row> {
    @Override
    public void collect(Row record) {
      builder.add(record);

      if (builder.currentSize() > tableMaxSize) {
        writeToEdges(builder.build());
        builder = new ArrowTableBuilder(schema.toArrowSchema(), runtime.getRootAllocator());
      }
    }

    @Override
    public void close() {
      writeToEdges(builder.build());
    }
  }

  @Override
  public TFunction getFunction() {
    return computeFunction;
  }
}
