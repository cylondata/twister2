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

import java.util.Collections;

import edu.iu.dsc.tws.api.compute.nodes.ICompute;
import edu.iu.dsc.tws.api.tset.fn.ComputeCollectorFunc;
import edu.iu.dsc.tws.api.tset.fn.ComputeFunc;
import edu.iu.dsc.tws.api.tset.fn.TFunction;
import edu.iu.dsc.tws.tset.env.StreamingTSetEnvironment;
import edu.iu.dsc.tws.tset.ops.ComputeCollectorOp;
import edu.iu.dsc.tws.tset.ops.ComputeOp;

public class SComputeTSet<O, I> extends SBaseTSet<O> {
  private TFunction<O, I> computeFunc;

  public SComputeTSet(StreamingTSetEnvironment tSetEnv, ComputeFunc<O, I> computeFunction,
                      int parallelism) {
    this(tSetEnv, "scompute", computeFunction, parallelism);
  }

  public SComputeTSet(StreamingTSetEnvironment tSetEnv, ComputeCollectorFunc<O, I> compOp,
                      int parallelism) {
    this(tSetEnv, "scomputec", compOp, parallelism);
  }

  public SComputeTSet(StreamingTSetEnvironment tSetEnv, String name,
                      ComputeFunc<O, I> computeFunction, int parallelism) {
    super(tSetEnv, name, parallelism);
    this.computeFunc = computeFunction;
  }

  public SComputeTSet(StreamingTSetEnvironment tSetEnv, String name,
                      ComputeCollectorFunc<O, I> compOp, int parallelism) {
    super(tSetEnv, name, parallelism);
    this.computeFunc = compOp;
  }

  @Override
  public SComputeTSet<O, I> setName(String name) {
    rename(name);
    return this;
  }


  @Override
  public ICompute<I> getINode() {
    // todo: fix empty map
    if (computeFunc instanceof ComputeFunc) {
      return new ComputeOp<>((ComputeFunc<O, I>) computeFunc, this, Collections.emptyMap());
    } else if (computeFunc instanceof ComputeCollectorFunc) {
      return new ComputeCollectorOp<>((ComputeCollectorFunc<O, I>) computeFunc, this,
          Collections.emptyMap());
    }

    throw new RuntimeException("Unknown function type for compute: " + computeFunc);

  }
}
