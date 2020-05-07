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
package edu.iu.dsc.tws.tset.sets.batch;

import edu.iu.dsc.tws.api.compute.nodes.INode;
import edu.iu.dsc.tws.api.tset.fn.SourceFunc;
import edu.iu.dsc.tws.api.tset.schema.PrimitiveSchemas;
import edu.iu.dsc.tws.api.tset.schema.Schema;
import edu.iu.dsc.tws.api.tset.table.Row;
import edu.iu.dsc.tws.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.tset.ops.RowSourceOp;

public class RowSourceTSet extends BatchRowTSetImpl {
  private SourceFunc<Row> source;

  public RowSourceTSet(BatchTSetEnvironment tSetEnv, SourceFunc<Row> src, int parallelism) {
    this(tSetEnv, "source", src, parallelism, PrimitiveSchemas.NULL);
  }

  public RowSourceTSet(BatchTSetEnvironment tSetEnv, String name,
                       SourceFunc<Row> src, int parallelism, Schema schema) {
    super(tSetEnv, name, parallelism, schema);
    this.source = src;
  }

  @Override
  public INode getINode() {
    return new RowSourceOp(source, this, getInputs());
  }
}
