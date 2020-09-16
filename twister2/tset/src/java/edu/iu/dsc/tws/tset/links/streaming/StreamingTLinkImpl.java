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

import java.util.concurrent.TimeUnit;

import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.tset.fn.ComputeCollectorFunc;
import edu.iu.dsc.tws.api.tset.fn.ComputeFunc;
import edu.iu.dsc.tws.api.tset.fn.SinkFunc;
import edu.iu.dsc.tws.api.tset.fn.TFunction;
import edu.iu.dsc.tws.api.tset.link.streaming.StreamingTLink;
import edu.iu.dsc.tws.api.tset.schema.Schema;
import edu.iu.dsc.tws.api.tset.sets.streaming.StreamingTupleTSet;
import edu.iu.dsc.tws.task.window.util.WindowParameter;
import edu.iu.dsc.tws.tset.env.StreamingEnvironment;
import edu.iu.dsc.tws.tset.links.BaseTLinkWithSchema;
import edu.iu.dsc.tws.tset.sets.streaming.SComputeTSet;
import edu.iu.dsc.tws.tset.sets.streaming.SKeyedTSet;
import edu.iu.dsc.tws.tset.sets.streaming.SSinkTSet;
import edu.iu.dsc.tws.tset.sets.streaming.WindowComputeTSet;

public abstract class StreamingTLinkImpl<T1, T0> extends BaseTLinkWithSchema<T1, T0>
    implements StreamingTLink<T1, T0> {

  private WindowParameter windowParameter;

  StreamingTLinkImpl(StreamingEnvironment env, String n, int sourceP, int targetP,
                     Schema schema) {
    super(env, n, sourceP, targetP, schema);
  }

  @Override
  public StreamingEnvironment getTSetEnv() {
    return (StreamingEnvironment) super.getTSetEnv();
  }

  private <P> WindowComputeTSet<P> window(String n) {
    WindowComputeTSet<P> set;
    if (n != null && !n.isEmpty()) {
      set = new WindowComputeTSet<>(getTSetEnv(), n, getTargetParallelism(),
          this.windowParameter, getSchema());
    } else {
      set = new WindowComputeTSet<>(getTSetEnv(), getTargetParallelism(),
          this.windowParameter, getSchema());
    }
    addChildToGraph(set);

    return set;
  }

  public <P> SComputeTSet<P> compute(String n, TFunction<T1, P> computeFunction) {
    SComputeTSet<P> set = new SComputeTSet<>(getTSetEnv(), n, computeFunction,
        getTargetParallelism(), getSchema());
    addChildToGraph(set);

    return set;
  }

  public <K, V> SKeyedTSet<K, V> computeToTuple(String n,
                                                TFunction<T1, Tuple<K, V>> computeFunction) {
    SKeyedTSet<K, V> set = new SKeyedTSet<>(getTSetEnv(), n, computeFunction,
        getTargetParallelism(), getSchema());
    addChildToGraph(set);

    return set;
  }

  @Override
  public <P> SComputeTSet<P> compute(ComputeFunc<T1, P> computeFunction) {
    return compute(null, computeFunction);
  }

  @Override
  public <P> SComputeTSet<P> compute(ComputeCollectorFunc<T1, P> computeFunction) {
    return compute(null, computeFunction);
  }

  @Override
  public <K, V> StreamingTupleTSet<K, V> computeToTuple(ComputeFunc<T1, Tuple<K, V>> computeFunc) {
    return computeToTuple(null, computeFunc);
  }

  @Override
  public <K, V> StreamingTupleTSet<K, V> computeToTuple(ComputeCollectorFunc<T1, Tuple<K, V>>
                                                            computeFunc) {
    return computeToTuple(null, computeFunc);
  }

  @Override
  public SSinkTSet<T1> sink(SinkFunc<T1> sinkFunction) {
    SSinkTSet<T1> sinkTSet = new SSinkTSet<>(getTSetEnv(), sinkFunction, getTargetParallelism(),
        getSchema());
    addChildToGraph(sinkTSet);
    return sinkTSet;
  }

  public <P> WindowComputeTSet<P> countWindow(long windowLen) {
    this.windowParameter = new WindowParameter();
    this.windowParameter.withTumblingCountWindow(windowLen);
    return window("w-count-tumbling-compute-prev");
  }

  public <P> WindowComputeTSet<P> countWindow(long windowLen, long slidingLen) {
    this.windowParameter = new WindowParameter();
    this.windowParameter.withSlidingingCountWindow(windowLen, slidingLen);
    return window("w-count-sliding-compute-prev");
  }

  public <P> WindowComputeTSet<P> timeWindow(long windowLen,
                                             TimeUnit windowLenTimeUnit) {
    this.windowParameter = new WindowParameter();
    this.windowParameter.withTumblingDurationWindow(windowLen, windowLenTimeUnit);
    return window("w-duration-tumbling-compute-prev");
  }

  public <P> WindowComputeTSet<P> timeWindow(long windowLen,
                                             TimeUnit windowLenTimeUnit,
                                             long slidingLen,
                                             TimeUnit slidingWindowTimeUnit) {
    this.windowParameter = new WindowParameter();
    this.windowParameter.withSlidingDurationWindow(windowLen, windowLenTimeUnit, slidingLen,
        slidingWindowTimeUnit);
    return window("w-duration-sliding-compute-prev");
  }
}
