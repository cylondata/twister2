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
package edu.iu.dsc.tws.api.tset.link;

import com.google.common.reflect.TypeToken;

import edu.iu.dsc.tws.api.comms.messaging.types.MessageType;
import edu.iu.dsc.tws.api.dataset.DataObject;
import edu.iu.dsc.tws.api.tset.TBase;
import edu.iu.dsc.tws.api.tset.TSetEnvironment;
import edu.iu.dsc.tws.api.tset.TSetUtils;
import edu.iu.dsc.tws.api.tset.fn.ComputeCollectorFunc;
import edu.iu.dsc.tws.api.tset.fn.ComputeFunc;
import edu.iu.dsc.tws.api.tset.fn.SinkFunc;
import edu.iu.dsc.tws.api.tset.ops.ComputeCollectorOp;
import edu.iu.dsc.tws.api.tset.ops.ComputeOp;
import edu.iu.dsc.tws.api.tset.ops.SinkOp;
import edu.iu.dsc.tws.api.tset.sets.CachedTSet;
import edu.iu.dsc.tws.api.tset.sets.ComputeTSet;
import edu.iu.dsc.tws.api.tset.sets.SinkTSet;
import edu.iu.dsc.tws.api.tset.sets.TSet;

/**
 * Base link impl for all the links
 *
 * @param <T1> output type from the comms
 * @param <T0> base type
 */
public abstract class BaseTLink<T1, T0> implements TLink<T1, T0> {

  /**
   * The TSet Env used for runtime operations
   */
  private TSetEnvironment tSetEnv;

  /**
   * Name of the data set
   */
  private String name;

  private int sourceParallelism;

  private int targetParallelism;

  public BaseTLink(TSetEnvironment env, String n) {
    this(env, n, env.getDefaultParallelism());
  }

  public BaseTLink(TSetEnvironment env, String n, int sourceP) {
    this(env, n, sourceP, sourceP);
  }

  public BaseTLink(TSetEnvironment env, String n, int sourceP, int targetP) {
    this.tSetEnv = env;
    this.name = n;
    this.sourceParallelism = sourceP;
    this.targetParallelism = targetP;
  }

  protected <P> ComputeTSet<P, T1> compute(String n, ComputeFunc<P, T1> computeFunction) {
    ComputeTSet<P, T1> set;
    if (n != null && !n.isEmpty()) {
      set = new ComputeTSet<>(tSetEnv, n, new ComputeOp<>(computeFunction), targetParallelism);
    } else {
      set = new ComputeTSet<>(tSetEnv, new ComputeOp<>(computeFunction), targetParallelism);
    }
    addChildToGraph(set);

    return set;
  }

  protected <P> ComputeTSet<P, T1> compute(String n, ComputeCollectorFunc<P, T1> computeFunction) {
    ComputeTSet<P, T1> set;
    if (n != null && !n.isEmpty()) {
      set = new ComputeTSet<>(tSetEnv, n, new ComputeCollectorOp<>(computeFunction),
          targetParallelism);
    } else {
      set = new ComputeTSet<>(tSetEnv, new ComputeCollectorOp<>(computeFunction),
          targetParallelism);
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
  public void sink(SinkFunc<T1> sinkFunction) {
    SinkTSet<T1> sinkTSet = new SinkTSet<>(tSetEnv, new SinkOp<>(sinkFunction), targetParallelism);
    addChildToGraph(sinkTSet);
    tSetEnv.run(sinkTSet);
  }

  @Override
  public <TO> CachedTSet<TO> cache() {
    CachedTSet<TO> cacheTSet = new CachedTSet<>(tSetEnv, targetParallelism);
    addChildToGraph(cacheTSet);

    DataObject<TO> output = tSetEnv.runAndGet(cacheTSet);
    cacheTSet.setData(output);

    return cacheTSet;
  }

  public String getName() {
    return name;
  }

  protected void rename(String n) {
    this.name = n;
  }

  protected Class getType() {
    TypeToken<T1> typeToken = new TypeToken<T1>(getClass()) {
    };
    return typeToken.getRawType();
  }

  //todo: this always return Object type!!!
  protected MessageType getMessageType() {
    return TSetUtils.getDataType(getType());
  }

  protected void addChildToGraph(TBase child) {
    tSetEnv.getGraph().addTSet(this, child);
  }

  public TSetEnvironment getTSetEnv() {
    return tSetEnv;
  }

  public int getSourceParallelism() {
    return sourceParallelism;
  }

  public int getTargetParallelism() {
    return targetParallelism;
  }

  @Override
  public String toString() {
    return "Tlink{" + getName()
        + " s=" + tSetEnv.getGraph().getPredecessors(this)
        + " t=" + tSetEnv.getGraph().getSuccessors(this)
        + "}";
  }
}
