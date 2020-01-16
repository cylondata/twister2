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

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import edu.iu.dsc.tws.api.comms.messaging.types.MessageType;
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
  private MessageType dType;

  StreamingTLinkImpl(StreamingTSetEnvironment env, String n, int sourceP, int targetP,
                     MessageType dataType) {
    super(env, n, sourceP, targetP);
    this.dType = dataType;
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

  private <P> WindowComputeTSet<P, Iterator<T1>> window(String n) {
    WindowComputeTSet<P, Iterator<T1>> set;
    if (n != null && !n.isEmpty()) {
      set = new WindowComputeTSet<>(getTSetEnv(), n, getTargetParallelism(),
          this.windowParameter);
    } else {
      set = new WindowComputeTSet<>(getTSetEnv(), getTargetParallelism(),
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

  public <P> WindowComputeTSet<P, Iterator<T1>> countWindow(long windowLen) {
    this.windowParameter = new WindowParameter();
    this.windowParameter.withTumblingCountWindow(windowLen);
    return window("w-count-tumbling-compute-prev");
  }

  public <P> WindowComputeTSet<P, Iterator<T1>> countWindow(long windowLen, long slidingLen) {
    this.windowParameter = new WindowParameter();
    this.windowParameter.withSlidingingCountWindow(windowLen, slidingLen);
    return window("w-count-sliding-compute-prev");
  }

  public <P> WindowComputeTSet<P, Iterator<T1>> timeWindow(long windowLen,
                                                           TimeUnit windowLenTimeUnit) {
    this.windowParameter = new WindowParameter();
    this.windowParameter.withTumblingDurationWindow(windowLen, windowLenTimeUnit);
    return window("w-duration-tumbling-compute-prev");
  }

  public <P> WindowComputeTSet<P, Iterator<T1>> timeWindow(long windowLen,
                                                           TimeUnit windowLenTimeUnit,
                                                           long slidingLen,
                                                           TimeUnit slidingWindowTimeUnit) {
    this.windowParameter = new WindowParameter();
    this.windowParameter.withSlidingDurationWindow(windowLen, windowLenTimeUnit, slidingLen,
        slidingWindowTimeUnit);
    return window("w-duration-sliding-compute-prev");
  }


  protected MessageType getDataType() {
    return dType;
  }

}
