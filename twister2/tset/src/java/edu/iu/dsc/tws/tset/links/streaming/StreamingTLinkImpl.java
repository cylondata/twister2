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


package edu.iu.dsc.tws.tset.links.streaming;

import java.util.List;

import edu.iu.dsc.tws.api.compute.IMessage;
import edu.iu.dsc.tws.api.tset.fn.ComputeCollectorFunc;
import edu.iu.dsc.tws.api.tset.fn.ComputeFunc;
import edu.iu.dsc.tws.api.tset.fn.SinkFunc;
import edu.iu.dsc.tws.api.tset.link.streaming.StreamingTLink;
import edu.iu.dsc.tws.task.window.util.WindowParameter;
import edu.iu.dsc.tws.tset.env.StreamingTSetEnvironment;
import edu.iu.dsc.tws.tset.links.BaseTLink;
import edu.iu.dsc.tws.tset.sets.streaming.SComputeTSet;
import edu.iu.dsc.tws.tset.sets.streaming.SSinkTSet;
import edu.iu.dsc.tws.tset.sets.streaming.WindowComputeTSet;

public abstract class StreamingTLinkImpl<T1, T0> extends BaseTLink<T1, T0>
    implements StreamingTLink<T1, T0> {

  private WindowParameter windowParameter;

  StreamingTLinkImpl(StreamingTSetEnvironment env, String n, int sourceP, int targetP) {
    super(env, n, sourceP, targetP);
  }

  @Override
  public StreamingTSetEnvironment getTSetEnv() {
    return (StreamingTSetEnvironment) super.getTSetEnv();
  }

  public <P> SComputeTSet<P, T1> compute(String n, ComputeFunc<P, T1> computeFunction) {
    SComputeTSet<P, T1> set;
    if (n != null && !n.isEmpty()) {
      set = new SComputeTSet<>(getTSetEnv(), n, computeFunction, getTargetParallelism());
    } else {
      set = new SComputeTSet<>(getTSetEnv(), computeFunction, getTargetParallelism());
    }
    addChildToGraph(set);

    return set;
  }

  public <P> WindowComputeTSet<P, List<IMessage<T1>>> window(String n,
                                                             ComputeFunc<P, List<IMessage<T1>>>
                                                                 computeFunction) {
    WindowComputeTSet<P, List<IMessage<T1>>> set;
    if (n != null && !n.isEmpty()) {
      set = new WindowComputeTSet<>(getTSetEnv(), n, computeFunction, getTargetParallelism(),
          this.windowParameter);
    } else {
      set = new WindowComputeTSet<>(getTSetEnv(), computeFunction, getTargetParallelism(),
          this.windowParameter);
    }
    addChildToGraph(set);

    return set;
  }

  public <P> SComputeTSet<P, T1> compute(String n, ComputeCollectorFunc<P, T1> computeFunction) {
    SComputeTSet<P, T1> set;
    if (n != null && !n.isEmpty()) {
      set = new SComputeTSet<>(getTSetEnv(), n, computeFunction, getTargetParallelism());
    } else {
      set = new SComputeTSet<>(getTSetEnv(), computeFunction, getTargetParallelism());
    }
    addChildToGraph(set);

    return set;
  }

  @Override
  public <P> SComputeTSet<P, T1> compute(ComputeFunc<P, T1> computeFunction) {
    return compute(null, computeFunction);
  }

  @Override
  public <P> SComputeTSet<P, T1> compute(ComputeCollectorFunc<P, T1> computeFunction) {
    return compute(null, computeFunction);
  }

  @Override
  public SSinkTSet<T1> sink(SinkFunc<T1> sinkFunction) {
    SSinkTSet<T1> sinkTSet = new SSinkTSet<>(getTSetEnv(), sinkFunction, getTargetParallelism());
    addChildToGraph(sinkTSet);

    return sinkTSet;
  }

  public <P> WindowComputeTSet<P, List<IMessage<T1>>> countWindow(
      long windowLen, ComputeFunc<P, List<IMessage<T1>>> computeFunction) {
    this.windowParameter = new WindowParameter();
    this.windowParameter.withTumblingCountWindow(windowLen);
    return window("wcompute", computeFunction);
  }

  public <P> SComputeTSet<P, T1> countWindow(int windowLen, int slidingLen,
                                             ComputeCollectorFunc<P, T1> computeFunction) {
    return compute(null, computeFunction);
  }

  public <P> SComputeTSet<P, T1> timeWindow(int windowLen,
                                            ComputeCollectorFunc<P, T1> computeFunction) {
    return compute(null, computeFunction);
  }

  public <P> SComputeTSet<P, T1> timeWindow(int windowLen, int slidingLen,
                                            ComputeCollectorFunc<P, T1> computeFunction) {
    return compute(null, computeFunction);
  }
}
