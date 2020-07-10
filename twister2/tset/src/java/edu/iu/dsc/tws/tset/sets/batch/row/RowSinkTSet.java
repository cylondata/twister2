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
package edu.iu.dsc.tws.tset.sets.batch.row;

import edu.iu.dsc.tws.api.compute.nodes.INode;
import edu.iu.dsc.tws.api.tset.TBase;
import edu.iu.dsc.tws.api.tset.fn.SinkFunc;
import edu.iu.dsc.tws.api.tset.schema.Schema;
import edu.iu.dsc.tws.api.tset.sets.AcceptingData;
import edu.iu.dsc.tws.api.tset.sets.StorableTBase;
import edu.iu.dsc.tws.common.table.Row;
import edu.iu.dsc.tws.tset.env.BatchEnvironment;
import edu.iu.dsc.tws.tset.ops.row.RowSinkOp;
import edu.iu.dsc.tws.tset.sets.BaseTSetWithSchema;

public class RowSinkTSet extends BaseTSetWithSchema<Row> implements AcceptingData<Row> {
  private SinkFunc<Row> sinkFunc;

  /**
   * Creates SinkTSet with the given parameters, the parallelism of the TSet is taken as 1
   *
   * @param tSetEnv The TSetEnv used for execution
   * @param s       The Sink function to be used
   */
  public RowSinkTSet(BatchEnvironment tSetEnv, SinkFunc<Row> s, Schema inputSchema) {
    this(tSetEnv, s, 1, inputSchema);
  }

  /**
   * Creates SinkTSet with the given parameters
   *
   * @param tSetEnv     The TSetEnv used for execution
   * @param sinkFn      The Sink function to be used
   * @param parallelism the parallelism of the sink
   */
  public RowSinkTSet(BatchEnvironment tSetEnv, SinkFunc<Row> sinkFn, int parallelism,
                     Schema inputSchema) {
    super(tSetEnv, "sink", parallelism, inputSchema);
    this.sinkFunc = sinkFn;
  }

  @Override
  public RowSinkTSet addInput(String key, StorableTBase<?> input) {
    getTSetEnv().addInput(getId(), input.getId(), key);
    return this;
  }

  @Override
  public INode getINode() {
    return new RowSinkOp(sinkFunc, this, getInputs());
  }

  @Override
  public TBase setName(String name) {
    rename(name);
    return this;
  }

  public RowSinkTSet withSchema(Schema schema) {
    this.setOutputSchema(schema);
    return this;
  }
}
