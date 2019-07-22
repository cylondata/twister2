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

import edu.iu.dsc.tws.api.task.nodes.INode;
import edu.iu.dsc.tws.api.tset.TSetEnvironment;
import edu.iu.dsc.tws.api.tset.TSetUtils;
import edu.iu.dsc.tws.api.tset.fn.SourceFunc;
import edu.iu.dsc.tws.api.tset.ops.SourceOp;

public class BatchSourceTSet<T> extends BatchBaseTSet<T> {
  private SourceFunc<T> source;

  public BatchSourceTSet(TSetEnvironment tSetEnv, SourceFunc<T> src, int parallelism) {
    super(tSetEnv, TSetUtils.generateName("source"), parallelism);
    this.source = src;
  }

  @Override
  public INode getINode() {
    return new SourceOp<>(source);
  }

  @Override
  public BatchSourceTSet<T> setName(String name) {
    rename(name);
    return this;
  }

}
