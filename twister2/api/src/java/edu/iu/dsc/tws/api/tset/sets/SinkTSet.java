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
import edu.iu.dsc.tws.api.tset.ops.SinkOp;

public class SinkTSet<T> extends BatchBaseTSet<T> {
  private SinkOp<T> sink;

  /**
   * Creates SinkTSet with the given parameters, the parallelism of the TSet is taken as 1
   *
   * @param tSetEnv The TSetEnv used for execution
   * @param s The Sink function to be used
   */
  public SinkTSet(TSetEnvironment tSetEnv, SinkOp<T> s) {
    this(tSetEnv, s, 1);
  }

  /**
   * Creates SinkTSet with the given parameters
   *
   * @param tSetEnv The TSetEnv used for execution
   * @param s The Sink function to be used
   * @param parallelism the parallelism of the sink
   */
  public SinkTSet(TSetEnvironment tSetEnv, SinkOp<T> s, int parallelism) {
    super(tSetEnv, TSetUtils.generateName("sink"), parallelism);
    this.sink = s;
  }

  @Override
  public ICompute getINode() {
    return sink;
  }

  @Override
  public SinkTSet<T> setName(String n) {
    rename(n);
    return this;
  }
}
