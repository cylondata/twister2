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

import java.util.Map;

import edu.iu.dsc.tws.api.compute.TaskContext;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.api.exceptions.Twister2RuntimeException;
import edu.iu.dsc.tws.api.resource.WorkerEnvironment;
import edu.iu.dsc.tws.api.tset.TSetConstants;
import edu.iu.dsc.tws.api.tset.fn.SourceFunc;
import edu.iu.dsc.tws.api.tset.schema.RowSchema;
import edu.iu.dsc.tws.common.table.ArrowTableBuilder;
import edu.iu.dsc.tws.common.table.Row;
import edu.iu.dsc.tws.common.table.TableBuilder;
import edu.iu.dsc.tws.common.table.arrow.TableRuntime;
import edu.iu.dsc.tws.tset.ops.SourceOp;
import edu.iu.dsc.tws.tset.sets.BaseTSet;

public class RowSourceOp extends SourceOp<Row> {
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


  public RowSourceOp() {
  }

  public RowSourceOp(SourceFunc<Row> src, BaseTSet originTSet,
                     Map<String, String> receivableTSets) {
    super(src, originTSet, receivableTSets);
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
  public void execute() {
    if (source.hasNext()) {
      // todo:: change source function to accept a row, so we don't have to allocate new
      // row every time
      Row tuple = source.next();
      builder.add(tuple);

      if (builder.currentSize() > tableMaxSize) {
        multiEdgeOpAdapter.writeToEdges(builder.build());
        builder = new ArrowTableBuilder(schema.toArrowSchema(), runtime.getRootAllocator());
      }
    } else {
      multiEdgeOpAdapter.writeToEdges(builder.build());
      builder = null;
      multiEdgeOpAdapter.writeEndToEdges();
    }
  }
}
