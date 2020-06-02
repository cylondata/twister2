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
package edu.iu.dsc.tws.tset.links.batch.row;

import java.util.Iterator;

import edu.iu.dsc.tws.api.tset.fn.ApplyFunc;
import edu.iu.dsc.tws.api.tset.fn.ComputeCollectorFunc;
import edu.iu.dsc.tws.api.tset.fn.FlatMapFunc;
import edu.iu.dsc.tws.api.tset.fn.MapFunc;
import edu.iu.dsc.tws.api.tset.fn.SinkFunc;
import edu.iu.dsc.tws.api.tset.link.batch.BatchRowTLink;
import edu.iu.dsc.tws.api.tset.sets.StorableTBase;
import edu.iu.dsc.tws.api.tset.sets.batch.BatchRowTSet;
import edu.iu.dsc.tws.common.table.Row;
import edu.iu.dsc.tws.api.tset.schema.RowSchema;
import edu.iu.dsc.tws.tset.env.BatchTSetEnvironment;
import edu.iu.dsc.tws.tset.env.CheckpointingTSetEnv;
import edu.iu.dsc.tws.tset.fn.row.RowFlatMapCompute;
import edu.iu.dsc.tws.tset.fn.row.RowForEachCompute;
import edu.iu.dsc.tws.tset.fn.row.RowMapCompute;
import edu.iu.dsc.tws.tset.links.BaseTLinkWithSchema;
import edu.iu.dsc.tws.tset.sets.BaseTSet;
import edu.iu.dsc.tws.tset.sets.batch.CheckpointedTSet;
import edu.iu.dsc.tws.tset.sets.batch.row.RowCachedTSet;
import edu.iu.dsc.tws.tset.sets.batch.row.RowComputeTSet;
import edu.iu.dsc.tws.tset.sets.batch.row.RowPersistedTSet;
import edu.iu.dsc.tws.tset.sets.batch.row.RowSinkTSet;
import edu.iu.dsc.tws.tset.sinks.CacheSingleSink;
import edu.iu.dsc.tws.tset.sinks.DiskPersistSingleSink;
import edu.iu.dsc.tws.tset.sources.DiskPartitionBackedSource;

public abstract class RowBatchTLinkImpl extends BaseTLinkWithSchema<Row, Row>
    implements BatchRowTLink {
  RowBatchTLinkImpl(BatchTSetEnvironment env, String n, int sourceP,
                    int targetP, RowSchema schema) {
    super(env, n, sourceP, targetP, schema);
  }

  RowBatchTLinkImpl(BatchTSetEnvironment env, String n, int sourceP, RowSchema schema) {
    super(env, n, sourceP, schema);
  }

  RowBatchTLinkImpl() {
  }

  @Override
  public BatchTSetEnvironment getTSetEnv() {
    return (BatchTSetEnvironment) super.getTSetEnv();
  }

  public RowComputeTSet compute(String n,
                                ComputeCollectorFunc<Row, Iterator<Row>> computeFunction) {
    RowComputeTSet set;
    if (n != null && !n.isEmpty()) {
      set = new RowComputeTSet(getTSetEnv(), n, computeFunction, getTargetParallelism(),
          (RowSchema) getSchema());
    } else {
      set = new RowComputeTSet(getTSetEnv(), computeFunction, getTargetParallelism(),
          (RowSchema) getSchema());
    }
    addChildToGraph(set);

    return set;
  }

  @Override
  public RowComputeTSet compute(ComputeCollectorFunc<Row, Iterator<Row>> computeFunction) {
    return compute(null, computeFunction);
  }

  @Override
  public BatchRowTSet map(MapFunc<Row, Row> mapFn) {
    return compute("map", new RowMapCompute(mapFn));
  }

  @Override
  public BatchRowTSet flatmap(FlatMapFunc<Row, Row> mapFn) {
    return compute("map", new RowFlatMapCompute(mapFn));
  }

  @Override
  public void forEach(ApplyFunc<Row> applyFunction) {
    BatchRowTSet lazyPersist = lazyForEach(applyFunction);
    getTSetEnv().run((BaseTSet) lazyPersist);
  }

  @Override
  public BatchRowTSet lazyForEach(ApplyFunc<Row> applyFunction) {
    return compute("foreach", new RowForEachCompute(applyFunction));
  }

  @Override
  public StorableTBase<Row> lazyCache() {
    RowCachedTSet cacheTSet = new RowCachedTSet(getTSetEnv(), new CacheSingleSink<Row>(),
        getTargetParallelism(), (RowSchema) getSchema());
    addChildToGraph(cacheTSet);
    return cacheTSet;
  }

  @Override
  public StorableTBase<Row> lazyPersist() {
    DiskPersistSingleSink<Row> diskPersistSingleSink = new DiskPersistSingleSink<>(
        this.getId()
    );
    RowPersistedTSet persistedTSet = new RowPersistedTSet(getTSetEnv(),
        diskPersistSingleSink, getTargetParallelism(), (RowSchema) getSchema());
    addChildToGraph(persistedTSet);
    return persistedTSet;
  }

  @Override
  public RowSinkTSet sink(SinkFunc<Row> sinkFunction) {
    RowSinkTSet sinkTSet = new RowSinkTSet(getTSetEnv(), sinkFunction,
        getTargetParallelism(), getSchema());
    addChildToGraph(sinkTSet);
    return sinkTSet;
  }

  /*
   * Returns the superclass @Storable<T> because, this class is used by both keyed and non-keyed
   * TSets. Hence, it produces both CachedTSet<T> as well as KeyedCachedTSet<K, V>
   */
  @Override
  public StorableTBase<Row> cache() {
    StorableTBase<Row> cacheTSet = lazyCache();
    getTSetEnv().run((BaseTSet) cacheTSet);
    return cacheTSet;
  }

  /*
   * Similar to cache, but stores data in disk rather than in memory.
   */
  @Override
  public StorableTBase<Row> persist() {
    // handling checkpointing
    if (getTSetEnv().isCheckpointingEnabled()) {
      String persistVariableName = this.getId() + "-persisted";
      CheckpointingTSetEnv chkEnv = (CheckpointingTSetEnv) getTSetEnv();
      Boolean persisted = chkEnv.initVariable(persistVariableName, false);
      if (persisted) {
        // create a source function with the capability to read from disk
        DiskPartitionBackedSource<Row> sourceFn = new DiskPartitionBackedSource<>(this.getId());

        // pass the source fn to the checkpointed tset (that would create a source tset from the
        // source function, the same way as a persisted tset. This preserves the order of tsets
        // that are being created in the checkpointed env)
        CheckpointedTSet<Row> checkTSet = new CheckpointedTSet<>(getTSetEnv(), sourceFn,
            this.getTargetParallelism(), getSchema());

        // adding checkpointed tset to the graph, so that the IDs would not change
        addChildToGraph(checkTSet);

        // run only the checkpointed tset so that it would populate the inputs in the executor
        getTSetEnv().runOne(checkTSet);

        return checkTSet;
      } else {
        StorableTBase<Row> storable = this.doPersist();
        chkEnv.updateVariable(persistVariableName, true);
        chkEnv.commit();
        return storable;
      }
    }
    return doPersist();
  }

  private StorableTBase<Row> doPersist() {
    StorableTBase<Row> lazyPersist = lazyPersist();
    getTSetEnv().run((BaseTSet) lazyPersist);
    return lazyPersist;
  }
}
