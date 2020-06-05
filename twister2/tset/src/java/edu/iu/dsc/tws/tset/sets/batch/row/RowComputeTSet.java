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

import java.util.Iterator;

import edu.iu.dsc.tws.api.compute.nodes.INode;
import edu.iu.dsc.tws.api.tset.fn.ComputeCollectorFunc;
import edu.iu.dsc.tws.api.tset.fn.ComputeFunc;
import edu.iu.dsc.tws.api.tset.fn.TFunction;
import edu.iu.dsc.tws.api.tset.schema.RowSchema;
import edu.iu.dsc.tws.common.table.Row;
import edu.iu.dsc.tws.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.tset.ops.row.RowComupteCollectorOp;

public class RowComputeTSet extends BatchRowTSetImpl {
  private TFunction<Row, Iterator<Row>> computeFunc;

  public RowComputeTSet(BatchTSetEnvironment tSetEnv, ComputeFunc<Row, Iterator<Row>> computeFn,
                     int parallelism, RowSchema inputSchema) {
    this(tSetEnv, "compute", computeFn, parallelism, inputSchema);
  }

  public RowComputeTSet(BatchTSetEnvironment tSetEnv,
                        ComputeCollectorFunc<Row, Iterator<Row>> computeFn, int parallelism,
                        RowSchema inputSchema) {
    this(tSetEnv, "computec", computeFn, parallelism, inputSchema);
  }

  public RowComputeTSet(BatchTSetEnvironment tSetEnv, String name,
                        ComputeFunc<Row, Iterator<Row>> computeFn,
                     int parallelism, RowSchema inputSchema) {
    super(tSetEnv, name, parallelism, inputSchema);
    this.computeFunc = computeFn;
  }

  public RowComputeTSet(BatchTSetEnvironment tSetEnv, String name,
                        ComputeCollectorFunc<Row, Iterator<Row>> computeFn,
                        int parallelism, RowSchema inputSchema) {
    super(tSetEnv, name, parallelism, inputSchema);
    this.computeFunc = computeFn;
  }

  @Override
  public INode getINode() {
    if (computeFunc instanceof ComputeCollectorFunc) {
      return new RowComupteCollectorOp((ComputeCollectorFunc<Row, Iterator<Row>>) computeFunc,
          this, getInputs());
    }

    throw new RuntimeException("Unknown function type for compute: " + computeFunc);
  }

  /**
   * Get the compute function associated with this TSet
   *
   * @return the compute function
   */
  public TFunction<Row, Iterator<Row>> getComputeFunc() {
    return computeFunc;
  }
}
