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

import edu.iu.dsc.tws.api.compute.nodes.ICompute;
import edu.iu.dsc.tws.api.tset.fn.ComputeCollectorFunc;
import edu.iu.dsc.tws.api.tset.fn.ComputeFunc;
import edu.iu.dsc.tws.api.tset.fn.TFunction;
import edu.iu.dsc.tws.api.tset.schema.Schema;
import edu.iu.dsc.tws.api.tset.sets.StorableTBase;
import edu.iu.dsc.tws.tset.env.BatchEnvironment;
import edu.iu.dsc.tws.tset.ops.ComputeCollectorOp;
import edu.iu.dsc.tws.tset.ops.ComputeOp;

public class ComputeTSet<O> extends BatchTSetImpl<O> {
  private TFunction<?, O> computeFunc;

  public ComputeTSet() {
    //non arg constructor needed for kryo
    super();
  }

  public ComputeTSet(BatchEnvironment tSetEnv, ComputeFunc<?, O> computeFn,
                     int parallelism, Schema inputSchema) {
    this(tSetEnv, "compute", computeFn, parallelism, inputSchema);
  }

  public ComputeTSet(BatchEnvironment tSetEnv, ComputeCollectorFunc<?, O> computeFn,
                     int parallelism, Schema inputSchema) {
    this(tSetEnv, "computec", computeFn, parallelism, inputSchema);
  }

  public ComputeTSet(BatchEnvironment tSetEnv, String name, ComputeFunc<?, O> computeFn,
                     int parallelism, Schema inputSchema) {
    super(tSetEnv, name, parallelism, inputSchema);
    this.computeFunc = computeFn;
  }

  public ComputeTSet(BatchEnvironment tSetEnv, String name,
                     ComputeCollectorFunc<?, O> computeFn, int parallelism, Schema inputSchema) {
    super(tSetEnv, name, parallelism, inputSchema);
    this.computeFunc = computeFn;
  }

  @Override
  public ComputeTSet<O> setName(String name) {
    rename(name);
    return this;
  }

  @Override
  public ComputeTSet<O> addInput(String key, StorableTBase<?> input) {
    return (ComputeTSet<O>) super.addInput(key, input);
  }

  @Override
  public ComputeTSet<O> withSchema(Schema schema) {
    return (ComputeTSet<O>) super.withSchema(schema);
  }

  @Override
  public ICompute<?> getINode() {

    if (computeFunc instanceof ComputeFunc) {
      return new ComputeOp<>((ComputeFunc<?, O>) computeFunc, this, getInputs());
    } else if (computeFunc instanceof ComputeCollectorFunc) {
      return new ComputeCollectorOp<>((ComputeCollectorFunc<?, O>) computeFunc, this,
          getInputs());
    }

    throw new RuntimeException("Unknown function type for compute: " + computeFunc);
  }

  /**
   * Get the compute function associated with this TSet
   *
   * @return the compute function
   */
  public TFunction<?, O> getComputeFunc() {
    return computeFunc;
  }
}
