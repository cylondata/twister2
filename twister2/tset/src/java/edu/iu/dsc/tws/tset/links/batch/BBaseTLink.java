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

package edu.iu.dsc.tws.tset.links.batch;

import edu.iu.dsc.tws.api.tset.fn.ComputeCollectorFunc;
import edu.iu.dsc.tws.api.tset.fn.ComputeFunc;
import edu.iu.dsc.tws.api.tset.fn.SinkFunc;
import edu.iu.dsc.tws.api.tset.link.batch.BatchTLink;
import edu.iu.dsc.tws.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.tset.links.BaseTLink;
import edu.iu.dsc.tws.tset.sets.batch.ComputeTSet;
import edu.iu.dsc.tws.tset.sets.batch.SinkTSet;

public abstract class BBaseTLink<T1, T0> extends BaseTLink<T1, T0>
    implements BatchTLink<T1, T0> {

  BBaseTLink(BatchTSetEnvironment env, String n, int sourceP, int targetP) {
    super(env, n, sourceP, targetP);
  }

  @Override
  public BatchTSetEnvironment getTSetEnv() {
    return (BatchTSetEnvironment) super.getTSetEnv();
  }

  public <P> ComputeTSet<P, T1> compute(String n, ComputeFunc<P, T1> computeFunction) {
    ComputeTSet<P, T1> set;
    if (n != null && !n.isEmpty()) {
      set = new ComputeTSet<>(getTSetEnv(), n, computeFunction, getTargetParallelism());
    } else {
      set = new ComputeTSet<>(getTSetEnv(), computeFunction, getTargetParallelism());
    }
    addChildToGraph(set);

    return set;
  }

  public <P> ComputeTSet<P, T1> compute(String n, ComputeCollectorFunc<P, T1> computeFunction) {
    ComputeTSet<P, T1> set;
    if (n != null && !n.isEmpty()) {
      set = new ComputeTSet<>(getTSetEnv(), n, computeFunction, getTargetParallelism());
    } else {
      set = new ComputeTSet<>(getTSetEnv(), computeFunction, getTargetParallelism());
    }
    addChildToGraph(set);

    return set;
  }

  @Override
  public <P> ComputeTSet<P, T1> compute(ComputeFunc<P, T1> computeFunction) {
    return compute(null, computeFunction);
  }

  @Override
  public <P> ComputeTSet<P, T1> compute(ComputeCollectorFunc<P, T1> computeFunction) {
    return compute(null, computeFunction);
  }

  @Override
  public SinkTSet<T1> lazySink(SinkFunc<T1> sinkFunction) {
    SinkTSet<T1> sinkTSet = new SinkTSet<>(getTSetEnv(), sinkFunction, getTargetParallelism());
    addChildToGraph(sinkTSet);

    return sinkTSet;
  }

  @Override
  public void sink(SinkFunc<T1> sinkFunction) {
    SinkTSet<T1> sinkTSet = lazySink(sinkFunction);
    getTSetEnv().run(sinkTSet);
  }
}
