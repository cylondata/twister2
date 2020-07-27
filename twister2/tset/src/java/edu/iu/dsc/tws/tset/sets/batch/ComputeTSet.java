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

public class ComputeTSet<O, I> extends BatchTSetImpl<O> {
  private TFunction<I, O> computeFunc;

  public ComputeTSet() {
    //non arg constructor needed for kryo
    super();
  }

  public ComputeTSet(BatchEnvironment tSetEnv, ComputeFunc<I, O> computeFn,
                     int parallelism, Schema inputSchema) {
    this(tSetEnv, "compute", computeFn, parallelism, inputSchema);
  }

  public ComputeTSet(BatchEnvironment tSetEnv, ComputeCollectorFunc<I, O> computeFn,
                     int parallelism, Schema inputSchema) {
    this(tSetEnv, "computec", computeFn, parallelism, inputSchema);
  }

  public ComputeTSet(BatchEnvironment tSetEnv, String name, ComputeFunc<I, O> computeFn,
                     int parallelism, Schema inputSchema) {
    super(tSetEnv, name, parallelism, inputSchema);
    this.computeFunc = computeFn;
  }

  public ComputeTSet(BatchEnvironment tSetEnv, String name,
                     ComputeCollectorFunc<I, O> computeFn, int parallelism, Schema inputSchema) {
    super(tSetEnv, name, parallelism, inputSchema);
    this.computeFunc = computeFn;
  }

  @Override
  public ComputeTSet<O, I> setName(String name) {
    rename(name);
    return this;
  }

  @Override
  public ComputeTSet<O, I> addInput(String key, StorableTBase<?> input) {
    return (ComputeTSet<O, I>) super.addInput(key, input);
  }

  @Override
  public ComputeTSet<O, I> withSchema(Schema schema) {
    return (ComputeTSet<O, I>) super.withSchema(schema);
  }

  @Override
  public ICompute<I> getINode() {

    if (computeFunc instanceof ComputeFunc) {
      return new ComputeOp<>((ComputeFunc<I, O>) computeFunc, this, getInputs());
    } else if (computeFunc instanceof ComputeCollectorFunc) {
      return new ComputeCollectorOp<>((ComputeCollectorFunc<I, O>) computeFunc, this,
          getInputs());
    }

    throw new RuntimeException("Unknown function type for compute: " + computeFunc);
  }

  /**
   * Get the compute function associated with this TSet
   *
   * @return the compute function
   */
  public TFunction<I, O> getComputeFunc() {
    return computeFunc;
  }
}
