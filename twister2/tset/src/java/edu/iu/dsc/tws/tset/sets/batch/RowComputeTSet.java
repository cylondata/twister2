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
import edu.iu.dsc.tws.api.tset.fn.ComputeCollectorFunc;
import edu.iu.dsc.tws.api.tset.fn.ComputeFunc;
import edu.iu.dsc.tws.api.tset.fn.TFunction;
import edu.iu.dsc.tws.api.tset.schema.Schema;
import edu.iu.dsc.tws.api.tset.table.Row;
import edu.iu.dsc.tws.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.tset.ops.ComputeCollectorOp;
import edu.iu.dsc.tws.tset.ops.RowComputeOp;

public class RowComputeTSet extends BatchRowTSetImpl {
  private TFunction<Row, Row> computeFunc;

  public RowComputeTSet(BatchTSetEnvironment tSetEnv, ComputeFunc<Row, Row> computeFn,
                     int parallelism, Schema inputSchema) {
    this(tSetEnv, "compute", computeFn, parallelism, inputSchema);
  }

  public RowComputeTSet(BatchTSetEnvironment tSetEnv, ComputeCollectorFunc<Row, Row> computeFn,
                     int parallelism, Schema inputSchema) {
    this(tSetEnv, "computec", computeFn, parallelism, inputSchema);
  }

  public RowComputeTSet(BatchTSetEnvironment tSetEnv, String name, ComputeFunc<Row, Row> computeFn,
                     int parallelism, Schema inputSchema) {
    super(tSetEnv, name, parallelism, inputSchema);
    this.computeFunc = computeFn;
  }

  public RowComputeTSet(BatchTSetEnvironment tSetEnv, String name,
                        ComputeCollectorFunc<Row, Row> computeFn,
                        int parallelism, Schema inputSchema) {
    super(tSetEnv, name, parallelism, inputSchema);
    this.computeFunc = computeFn;
  }
  @Override
  public INode getINode() {
    if (computeFunc instanceof ComputeFunc) {
      return new RowComputeOp((ComputeFunc<Row, Row>) computeFunc, this, getInputs());
    } else if (computeFunc instanceof ComputeCollectorFunc) {
      return new ComputeCollectorOp<>((ComputeCollectorFunc<Row, Row>) computeFunc, this,
          getInputs());
    }

    throw new RuntimeException("Unknown function type for compute: " + computeFunc);
  }

  /**
   * Get the compute function associated with this TSet
   *
   * @return the compute function
   */
  public TFunction<Row, Row> getComputeFunc() {
    return computeFunc;
  }
}
