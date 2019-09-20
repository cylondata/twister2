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

package edu.iu.dsc.tws.tset.sets.streaming;

import edu.iu.dsc.tws.api.compute.nodes.ICompute;
import edu.iu.dsc.tws.tset.env.StreamingTSetEnvironment;
import edu.iu.dsc.tws.tset.ops.BaseComputeOp;
import edu.iu.dsc.tws.tset.ops.ComputeCollectorOp;
import edu.iu.dsc.tws.tset.ops.ComputeOp;

public class SComputeTSet<O, I> extends SBaseTSet<O> {
  private BaseComputeOp<I> computeOp;

  public SComputeTSet(StreamingTSetEnvironment tSetEnv, ComputeOp<O, I> computeFunction,
                      int parallelism) {
    this(tSetEnv, "scompute", computeFunction, parallelism);
  }

  public SComputeTSet(StreamingTSetEnvironment tSetEnv, ComputeCollectorOp<O, I> compOp,
                      int parallelism) {
    this(tSetEnv, "scomputec", compOp, parallelism);
  }

  public SComputeTSet(StreamingTSetEnvironment tSetEnv, String name,
                      ComputeOp<O, I> computeFunction, int parallelism) {
    super(tSetEnv, name, parallelism);
    this.computeOp = computeFunction;
  }

  public SComputeTSet(StreamingTSetEnvironment tSetEnv, String name,
                      ComputeCollectorOp<O, I> compOp, int parallelism) {
    super(tSetEnv, name, parallelism);
    this.computeOp = compOp;
  }

  @Override
  public SComputeTSet<O, I> setName(String name) {
    rename(name);
    return this;
  }


  @Override
  public ICompute<I> getINode() {
    return computeOp;
  }
}
