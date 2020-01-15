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

import edu.iu.dsc.tws.api.comms.messaging.types.MessageType;
import edu.iu.dsc.tws.api.comms.messaging.types.MessageTypes;
import edu.iu.dsc.tws.api.tset.fn.ComputeCollectorFunc;
import edu.iu.dsc.tws.api.tset.fn.ComputeFunc;
import edu.iu.dsc.tws.api.tset.fn.SinkFunc;
import edu.iu.dsc.tws.api.tset.link.TLink;
import edu.iu.dsc.tws.api.tset.link.batch.BatchTLink;
import edu.iu.dsc.tws.api.tset.sets.StorableTBase;
import edu.iu.dsc.tws.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.tset.env.CheckpointingTSetEnv;
import edu.iu.dsc.tws.tset.links.BaseTLink;
import edu.iu.dsc.tws.tset.sets.BaseTSet;
import edu.iu.dsc.tws.tset.sets.batch.CheckpointedTSet;
import edu.iu.dsc.tws.tset.sets.batch.ComputeTSet;
import edu.iu.dsc.tws.tset.sets.batch.SinkTSet;
import edu.iu.dsc.tws.tset.sources.DiskPartitionBackedSource;

public abstract class BatchTLinkImpl<T1, T0> extends BaseTLink<T1, T0>
    implements BatchTLink<T1, T0> {

  private MessageType dType = MessageTypes.OBJECT;

  BatchTLinkImpl(BatchTSetEnvironment env, String n, int sourceP, int targetP) {
    super(env, n, sourceP, targetP);
  }

  protected BatchTLinkImpl() {
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
  public StorableTBase<T0> cache() {
    StorableTBase<T0> cacheTSet = lazyCache();
    getTSetEnv().run((BaseTSet) cacheTSet);
    return cacheTSet;
  }

  /*
   * Similar to cache, but stores data in disk rather than in memory.
   */
  @Override
  public StorableTBase<T0> persist() {
    // handling checkpointing
    if (getTSetEnv().isCheckpointingEnabled()) {
      String persistVariableName = this.getId() + "-persisted";
      CheckpointingTSetEnv chkEnv = (CheckpointingTSetEnv) getTSetEnv();
      Boolean persisted = chkEnv.initVariable(persistVariableName, false);
      if (persisted) {
        // create a source function with the capability to read from disk
        DiskPartitionBackedSource<T0> sourceFn = new DiskPartitionBackedSource<>(this.getId());

        // pass the source fn to the checkpointed tset (that would create a source tset from the
        // source function, the same way as a persisted tset. This preserves the order of tsets
        // that are being created in the checkpointed env)
        CheckpointedTSet<T0> checkTSet = new CheckpointedTSet<>(getTSetEnv(), sourceFn,
            this.getTargetParallelism());

        // adding checkpointed tset to the graph, so that the IDs would not change
        addChildToGraph(checkTSet);

        // run only the checkpointed tset so that it would populate the inputs in the executor
        getTSetEnv().runOne(checkTSet);

        return checkTSet;
      } else {
        StorableTBase<T0> storable = this.doPersist();
        chkEnv.updateVariable(persistVariableName, true);
        chkEnv.commit();
        return storable;
      }
    }
    return doPersist();
  }

  @Override
  public TLink<T1, T0> withDataType(MessageType dataType) {
    this.dType = dataType;
    return this;
  }

  protected MessageType getDataType() {
    return this.dType;
  }

  private StorableTBase<T0> doPersist() {
    StorableTBase<T0> lazyPersist = lazyPersist();
    getTSetEnv().run((BaseTSet) lazyPersist);
    return lazyPersist;
  }
}
