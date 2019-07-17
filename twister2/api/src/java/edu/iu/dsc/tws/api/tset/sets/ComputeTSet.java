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
package edu.iu.dsc.tws.api.tset.sets;

import edu.iu.dsc.tws.api.task.nodes.ICompute;
import edu.iu.dsc.tws.api.tset.TSetEnvironment;
import edu.iu.dsc.tws.api.tset.TSetUtils;
import edu.iu.dsc.tws.api.tset.fn.ComputeFunction;
import edu.iu.dsc.tws.api.tset.ops.ComputeOp;

public class ComputeTSet<O, I> extends BatchBaseTSet<O> {
  private ComputeFunction<O, I> computeFunction;

  public ComputeTSet(TSetEnvironment tSetEnv, ComputeFunction<O, I> computeFunction,
                     int parallelism) {
    super(tSetEnv, TSetUtils.generateName("compute"), parallelism);
    this.computeFunction = computeFunction;
  }

  @Override
  public TSet<O> setName(String name) {
    rename(name);
    return this;
  }


  @Override
  protected ICompute getTask() {
    return new ComputeOp<>(computeFunction);
  }
}
