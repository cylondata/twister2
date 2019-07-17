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
import edu.iu.dsc.tws.api.tset.TSetGraph;
import edu.iu.dsc.tws.api.tset.TSetUtils;
import edu.iu.dsc.tws.api.tset.fn.Sink;
import edu.iu.dsc.tws.api.tset.ops.SinkOp;

public class SinkTSet<T> extends BatchBaseTSet<T> {
  private Sink<T> sink;

  /**
   * Creates SinkTSet with the given parameters, the parallelism of the TSet is taken as 1
   *
   * @param tSetEnv The TSetEnv used for execution
   * @param s The Sink function to be used
   */
  public SinkTSet(TSetEnvironment tSetEnv, Sink<T> s) {
    this(tSetEnv, s, 1);
  }

  /**
   * Creates SinkTSet with the given parameters
   *
   * @param tSetEnv The TSetEnv used for execution
   * @param s The Sink function to be used
   * @param parallelism the parallelism of the sink
   */
  public SinkTSet(TSetEnvironment tSetEnv, Sink<T> s, int parallelism) {
    super(tSetEnv, TSetUtils.generateName("sink"), parallelism);
    this.sink = s;
  }

  @Override
  public void build(TSetGraph tSetGraph) {
//    boolean isIterable = TSetUtils.isIterableInput(parent, tSetEnv.getTSetBuilder().getOpMode());
//    boolean keyed = TSetUtils.isKeyedInput(parent);
//    // lets override the parallelism
//    //int p = calculateParallelism(parent);
//    ComputeConnection connection =
//    tSetEnv.getTSetBuilder().getTaskGraphBuilder().addSink(getName(),
//        new SinkOp<>(sink, isIterable, keyed), parallel);
//    parent.buildConnection(connection);
//    return true;

    SinkOp<T> sinkOp = new SinkOp<>(sink, false, false);
    tSetGraph.getDfwGraphBuilder().addSink(getName(), sinkOp, getParallelism());
  }

  @Override
  protected ICompute getTask() {
    throw new UnsupportedOperationException("sink would not have an icompute task");
  }

  @Override
  public SinkTSet<T> setName(String n) {
    rename(n);
    return this;
  }
}
