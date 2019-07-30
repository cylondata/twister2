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

package edu.iu.dsc.tws.api.tset.sets.batch;

import edu.iu.dsc.tws.api.task.nodes.ICompute;
import edu.iu.dsc.tws.api.tset.TSetEnvironment;
import edu.iu.dsc.tws.api.tset.TSetUtils;
import edu.iu.dsc.tws.api.tset.ops.BaseComputeOp;
import edu.iu.dsc.tws.api.tset.ops.ComputeCollectorOp;
import edu.iu.dsc.tws.api.tset.ops.ComputeOp;

public class ComputeTSet<O, I> extends BBaseTSet<O> {
  private BaseComputeOp<I> computeOp;

  public ComputeTSet(TSetEnvironment tSetEnv, ComputeOp<O, I> computeFunction,
                     int parallelism) {
    this(tSetEnv, TSetUtils.generateName("compute"), computeFunction, parallelism);
  }

  public ComputeTSet(TSetEnvironment tSetEnv, ComputeCollectorOp<O, I> compOp,
                     int parallelism) {
    this(tSetEnv, TSetUtils.generateName("computec"), compOp, parallelism);
  }

  public ComputeTSet(TSetEnvironment tSetEnv, String name, ComputeOp<O, I> computeFunction,
                     int parallelism) {
    super(tSetEnv, name, parallelism);
    this.computeOp = computeFunction;
  }

  public ComputeTSet(TSetEnvironment tSetEnv, String name, ComputeCollectorOp<O, I> compOp,
                     int parallelism) {
    super(tSetEnv, name, parallelism);
    this.computeOp = compOp;
  }

  @Override
  public ComputeTSet<O, I> setName(String name) {
    rename(name);
    return this;
  }


  @Override
  public ICompute<I> getINode() {
    return computeOp;
  }
}
