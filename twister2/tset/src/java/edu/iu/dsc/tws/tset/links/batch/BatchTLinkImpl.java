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

import edu.iu.dsc.tws.api.tset.Storable;
import edu.iu.dsc.tws.api.tset.fn.ComputeCollectorFunc;
import edu.iu.dsc.tws.api.tset.fn.ComputeFunc;
import edu.iu.dsc.tws.api.tset.fn.SinkFunc;
import edu.iu.dsc.tws.api.tset.link.batch.BatchTLink;
import edu.iu.dsc.tws.checkpointing.util.CheckpointingConfigurations;
import edu.iu.dsc.tws.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.tset.env.CheckpointingTSetEnv;
import edu.iu.dsc.tws.tset.links.BaseTLink;
import edu.iu.dsc.tws.tset.sets.BaseTSet;
import edu.iu.dsc.tws.tset.sets.batch.CheckpointedTSet;
import edu.iu.dsc.tws.tset.sets.batch.ComputeTSet;
import edu.iu.dsc.tws.tset.sets.batch.SinkTSet;
import edu.iu.dsc.tws.tset.sets.batch.SourceTSet;
import edu.iu.dsc.tws.tset.sources.DiskPartitionBackedSource;

public abstract class BatchTLinkImpl<T1, T0> extends BaseTLink<T1, T0>
    implements BatchTLink<T1, T0> {

  BatchTLinkImpl(BatchTSetEnvironment env, String n, int sourceP, int targetP) {
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
  public SinkTSet<T1> sink(SinkFunc<T1> sinkFunction) {
    SinkTSet<T1> sinkTSet = new SinkTSet<>(getTSetEnv(), sinkFunction, getTargetParallelism());
    addChildToGraph(sinkTSet);

    return sinkTSet;
  }

  /*
   * Returns the superclass @Storable<T> because, this class is used by both keyed and non-keyed
   * TSets. Hence, it produces both CachedTSet<T> as well as KeyedCachedTSet<K, V>
   */
  @Override
  public Storable<T0> cache() {
    Storable<T0> cacheTSet = lazyCache();
    getTSetEnv().run((BaseTSet) cacheTSet);
    return cacheTSet;
  }

  /*
   * Similar to cache, but stores data in disk rather than in memory.
   * (This is for non-keyed operations! For keyed operations this will be overridden)
   */
  @Override
  public Storable<T0> persist() {
    // handling checkpointing
    if (CheckpointingConfigurations.isCheckpointingEnabled(getTSetEnv().getConfig())
        && getTSetEnv() instanceof CheckpointingTSetEnv) {
      String persistVariableName = this.getId() + "-persisted";
      CheckpointingTSetEnv chkEnv = (CheckpointingTSetEnv) getTSetEnv();
      Boolean persisted = chkEnv.initVariable(persistVariableName, false);
      if (persisted) {
        // create a source with the capability to read from disk
        final SourceTSet<T0> persistedSource = getTSetEnv().createSource(
            new DiskPartitionBackedSource<>(this.getId()), this.getTargetParallelism());

        return new CheckpointedTSet<>(getTSetEnv(), this.getTargetParallelism(), persistedSource);
      } else {
        Storable<T0> storable = this.doPersist();
        chkEnv.updateVariable(persistVariableName, true);
        chkEnv.commit();
        return storable;
      }
    }
    return doPersist();
  }

  private Storable<T0> doPersist() {
    Storable<T0> lazyPersist = lazyPersist();
    getTSetEnv().run((BaseTSet) lazyPersist);
    return lazyPersist;
  }
}
